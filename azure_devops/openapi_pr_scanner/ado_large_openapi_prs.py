#!/usr/bin/env python3
"""Find Azure DevOps PRs changing large OpenAPI JSON contracts (read-only)."""
import asyncio
import base64
import csv
import json
import logging
import os
import random
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote

import aiohttp
from dateutil.relativedelta import relativedelta
from tqdm import tqdm

# ---------- Configuration (environment variables) ----------
ADO_ORG = os.environ.get('ADO_ORG', '').strip()
ADO_PROJECT = os.environ.get('ADO_PROJECT', '').strip()
ADO_REPO = os.environ.get('ADO_REPO', '').strip()  # ID or repo name
ADO_PAT = os.environ.get('ADO_PAT', '').strip()
ADO_BASE_URL = os.environ.get('ADO_BASE_URL', 'https://dev.azure.com').rstrip('/')
# For on-premises, specify complete collection URL, e.g. https://ado.example.com/tfs/DefaultCollection
ADO_COLLECTION_URL = os.environ.get('ADO_COLLECTION_URL', '').strip().rstrip('/')
MONTHS = int(os.getenv('MONTHS', '6'))
MIN_FIELDS = int(os.getenv('MIN_FIELDS', '20'))
COUNT_MODE = os.getenv('COUNT_MODE', 'operation').strip().lower()  # operation or document
MAX_CHANGED_FILES = int(os.getenv('MAX_CHANGED_FILES', '3'))
CONCURRENCY = int(os.getenv('CONCURRENCY', '3'))
RPS = float(os.getenv('RPS', '2'))
OUTPUT_DIR = Path(os.getenv('OUTPUT_DIR', '.'))
API_VERSION = os.getenv('ADO_API_VERSION', '6.1').strip()
MAX_RETRIES = 5
MAX_BLOB_BYTES = 3_000_000

LOG = logging.getLogger('ado-audit')


class APIError(Exception):
    pass


class RateLimiter:
    """Global sliding-window limiter shared by all workers."""
    def __init__(self, rate):
        self.rate = rate
        self.lock = asyncio.Lock()
        self.calls = deque()

    async def wait(self):
        async with self.lock:
            while True:
                now = time.monotonic()
                while self.calls and now - self.calls[0] >= 1:
                    self.calls.popleft()
                if len(self.calls) < max(1, int(self.rate)):
                    self.calls.append(now)
                    return
                await asyncio.sleep(max(.01, 1 - (now - self.calls[0])))


class ADO:
    def __init__(self, session):
        self.session = session
        self.limiter = RateLimiter(RPS)
        if ADO_COLLECTION_URL:
            base = f'{ADO_COLLECTION_URL}/{quote(ADO_PROJECT, safe="")}'
        else:
            base = f'{ADO_BASE_URL}/{quote(ADO_ORG, safe="")}/{quote(ADO_PROJECT, safe="")}'
        self.root = base + '/_apis/git/repositories/' + quote(ADO_REPO, safe='')
        self.ui_base = base
        self.n_requests = 0

    async def get(self, path, params=None, binary=False):
        params = {'api-version': API_VERSION, **(params or {})}
        url = self.root + path
        for attempt in range(MAX_RETRIES + 1):
            await self.limiter.wait()
            self.n_requests += 1
            try:
                async with self.session.get(url, params=params, headers={'Accept': 'application/octet-stream' if binary else 'application/json'}) as resp:
                    if resp.status in (429, 500, 502, 503, 504) and attempt < MAX_RETRIES:
                        retry_after = resp.headers.get('Retry-After')
                        try:
                            delay = max(0.0, float(retry_after)) if retry_after else min(30, 2 ** attempt + random.uniform(0, 1))
                        except ValueError:
                            delay = min(30, 2 ** attempt + random.uniform(0, 1))
                        LOG.warning('HTTP %s, повтор через %.1f сек: %s', resp.status, delay, path)
                        await asyncio.sleep(delay)
                        continue
                    if resp.status != 200:
                        body = (await resp.text())[:350]
                        raise APIError(f'HTTP {resp.status} {path}: {body}')
                    if binary:
                        if resp.content_length and resp.content_length > MAX_BLOB_BYTES:
                            raise APIError(f'JSON blob > {MAX_BLOB_BYTES} bytes: {path}')
                        payload = await resp.content.read(MAX_BLOB_BYTES + 1)
                        if len(payload) > MAX_BLOB_BYTES:
                            raise APIError(f'JSON blob > {MAX_BLOB_BYTES} bytes: {path}')
                        return payload
                    return await resp.json(content_type=None)
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt >= MAX_RETRIES:
                    raise APIError(f'Network failure {path}: {exc}') from exc
                await asyncio.sleep(min(30, 2 ** attempt + random.uniform(0, 1)))
        raise APIError(f'Retries exhausted: {path}')

    async def list_prs(self, date_from, date_to):
        results = []
        offset = 0
        page_size = 100
        while True:
            payload = await self.get('/pullrequests', {
                'searchCriteria.status': 'all',
                'searchCriteria.queryTimeRangeType': 'created',
                'searchCriteria.minTime': date_from.isoformat(),
                'searchCriteria.maxTime': date_to.isoformat(),
                '$skip': offset,
                '$top': page_size,
            })
            page = payload['value']
            results.extend(page)
            LOG.info('Загружено PR: %d', len(results))
            if len(page) < page_size:
                return results
            offset += len(page)

    async def last_iteration_id(self, pr_id):
        obj = await self.get(f'/pullRequests/{pr_id}/iterations')
        values = obj['value']
        if not values:
            return None
        return max(v['id'] for v in values)

    async def changes_first_four(self, pr_id, iteration):
        return await self.get(f'/pullRequests/{pr_id}/iterations/{iteration}/changes', {
            '$compareTo': 0,
            '$top': MAX_CHANGED_FILES + 1,
            '$skip': 0,
        })

    async def blob(self, sha):
        return await self.get(f'/blobs/{sha}', {'$format': 'octetstream'}, binary=True)

    def pr_link(self, pr):
        # Azure DevOps web UI, NOT REST API.
        repo_name = pr.get('repository', {}).get('name') or ADO_REPO
        return f'{self.ui_base}/_git/{quote(repo_name, safe="")}/pullrequest/{pr["pullRequestId"]}'


def resolve_ref(root, ref):
    if not isinstance(ref, str) or not ref.startswith('#/'):
        return None
    node = root
    for part in ref[2:].split('/'):
        if not isinstance(node, dict):
            return None
        node = node.get(part.replace('~1', '/').replace('~0', '~'))
        if node is None:
            return None
    return node


def field_count(schema, spec, active=None):
    """Count declared properties, including references, composed schemas and arrays.

    Object fields are counted at each nesting level. Recursion cycles are stopped.
    For alternative shapes the largest option is used (not all options added).
    """
    active = active or frozenset()
    if not isinstance(schema, dict):
        return 0
    ref = schema.get('$ref')
    if ref:
        if ref in active:
            return 0
        target = resolve_ref(spec, ref)
        return field_count(target, spec, active | {ref}) if target is not None else 0
    props = schema.get('properties') or {}
    total = sum(1 + field_count(p, spec, active) for p in props.values() if isinstance(p, dict))
    total += sum(field_count(s, spec, active) for s in schema.get('allOf', []))
    for key in ('oneOf', 'anyOf'):
        total += max((field_count(s, spec, active) for s in schema.get(key, [])), default=0)
    total += field_count(schema.get('items'), spec, active)
    # Dictionaries / maps with structured values (additionalProperties: {...})
    total += field_count(schema.get('additionalProperties'), spec, active)
    return total


def dereference(value, spec):
    seen = set()
    while isinstance(value, dict) and '$ref' in value:
        ref = value['$ref']
        if ref in seen:
            return {}
        seen.add(ref)
        value = resolve_ref(spec, ref)
    return value if isinstance(value, dict) else {}


def parameter_count(param, spec):
    param = dereference(param, spec)
    if not param:
        return 0
    value = 0
    if param.get('in') == 'body':  # Swagger 2
        return field_count(param.get('schema'), spec)
    if param.get('in') not in ('query', 'path', 'header', 'cookie', 'formData'):
        return 0
    value = 1 + field_count(param.get('schema'), spec)
    for media in (param.get('content') or {}).values():
        value = max(value, 1 + field_count(media.get('schema'), spec))
    return value


def request_count(op, path_item, spec):
    body = dereference(op.get('requestBody') or {}, spec)
    body_fields = max((field_count(media.get('schema'), spec)
                       for media in (body.get('content') or {}).values()), default=0)
    # Path-level parameters are inherited; operation-level ones may override them.
    params = {}
    for param in (path_item.get('parameters') or []) + (op.get('parameters') or []):
        resolved = dereference(param, spec)
        if resolved:
            params[(resolved.get('name'), resolved.get('in'))] = resolved
    body_from_params = 0
    other_fields = 0
    for param in params.values():
        if param.get('in') == 'body':
            body_from_params = max(body_from_params, parameter_count(param, spec))
        else:
            other_fields += parameter_count(param, spec)
    return max(body_fields, body_from_params) + other_fields


def response_count(op, spec):
    # Different status codes / media types are alternatives, not simultaneous outputs.
    total = 0
    for status, raw_response in (op.get('responses') or {}).items():
        if not (str(status).startswith('2') or str(status).lower() == 'default'):
            continue
        response = dereference(raw_response, spec)
        candidate = field_count(response.get('schema'), spec)  # Swagger 2.0
        for media in (response.get('content') or {}).values():
            candidate = max(candidate, field_count(media.get('schema'), spec))
        # Named response headers are also part of the output contract.
        candidate += len(response.get('headers') or {})
        total = max(total, candidate)
    return total


def analyze_openapi(spec):
    if not isinstance(spec, dict) or not ('openapi' in spec or 'swagger' in spec):
        return None
    biggest = None
    sum_fields = 0
    operations = 0
    for path, raw_path_item in (spec.get('paths') or {}).items():
        path_item = dereference(raw_path_item, spec)
        for verb in ('get', 'post', 'put', 'patch', 'delete', 'options', 'head', 'trace'):
            op = path_item.get(verb)
            if not isinstance(op, dict):
                continue
            inp = request_count(op, path_item, spec)
            out = response_count(op, spec)
            operations += 1
            sum_fields += inp + out
            if biggest is None or inp + out > biggest['fields']:
                biggest = {'operation': f'{verb.upper()} {path}', 'request_fields': inp,
                           'response_fields': out, 'fields': inp + out}
    if biggest is not None:
        biggest['document_total_fields'] = sum_fields
        biggest['operation_count'] = operations
    return biggest

async def inspect_pr(api, pr):
    pr_id = pr['pullRequestId']
    link = api.pr_link(pr)
    base = {'pr_id': pr_id, 'url': link, 'title': pr.get('title', ''),
            'created': pr.get('creationDate', ''), 'status': pr.get('status', '')}
    try:
        iteration = await api.last_iteration_id(pr_id)
        if iteration is None:
            return 'error', {**base, 'reason': 'Нет итераций'}
        changes = await api.changes_first_four(pr_id, iteration)
        entries = changes.get('changeEntries', [])
        # ADO provides the first 4 changes. This is enough to classify >3.
        if len(entries) > MAX_CHANGED_FILES or changes.get('nextSkip', 0):
            return 'skipped', {**base, 'reason': f'Более {MAX_CHANGED_FILES} изменённых файлов'}
        matches = []
        json_files = 0
        openapi_files = 0
        for entry in entries:
            change_type = str(entry.get('changeType', '')).lower()
            if not any(word in change_type for word in ('add', 'edit', 'rename', 'undelete')):
                continue
            item = entry.get('item') or {}
            file_path = item.get('path', '')
            if not file_path.lower().endswith('.json') or item.get('isFolder'):
                continue
            json_files += 1
            sha = item.get('objectId')
            if not sha:
                # Do not pretend we checked JSON when there is no blob identifier.
                return 'error', {**base, 'reason': f'Нет objectId: {file_path}'}
            raw = await api.blob(sha)
            try:
                spec = json.loads(raw.decode('utf-8-sig'))
            except (UnicodeDecodeError, json.JSONDecodeError):
                LOG.debug('PR #%s: %s — невалидный JSON', pr_id, file_path)
                continue
            stats = analyze_openapi(spec)
            if stats:
                openapi_files += 1
            score = (stats['document_total_fields'] if COUNT_MODE == 'document' else stats['fields']) if stats else 0
            if stats and score >= MIN_FIELDS:
                matches.append({'file': file_path, **stats})
        if matches:
            return 'found', {**base, 'matches': matches}
        reason = ('Нет изменённых JSON-файлов' if not json_files else
                  'JSON-файлы не распознаны как OpenAPI' if not openapi_files else
                  'OpenAPI есть, но порог полей не достигнут')
        LOG.info('PR #%s: %s (JSON: %d, OpenAPI: %d)', pr_id, reason, json_files, openapi_files)
        return 'no_match', {**base, 'reason': reason}
    except Exception as exc:
        LOG.error('PR #%s: %s', pr_id, exc)
        return 'error', {**base, 'reason': str(exc)}


def write_csv(path, rows, columns):
    with path.open('w', newline='', encoding='utf-8-sig') as fh:
        writer = csv.DictWriter(fh, fieldnames=columns, extrasaction='ignore', delimiter=';')
        writer.writeheader()
        writer.writerows(rows)


async def main():
    if not all((ADO_PROJECT, ADO_REPO, ADO_PAT)) or not (ADO_ORG or ADO_COLLECTION_URL):
        raise SystemExit('Укажите ADO_PROJECT, ADO_REPO, ADO_PAT и одно из ADO_ORG/ADO_COLLECTION_URL')
    if COUNT_MODE not in ('operation', 'document'):
        raise SystemExit('COUNT_MODE должен быть operation или document')
    if CONCURRENCY < 1 or RPS < 1 or MAX_CHANGED_FILES < 1:
        raise SystemExit('CONCURRENCY, RPS, MAX_CHANGED_FILES должны быть >= 1')
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)-7s | %(message)s',
                        handlers=[logging.FileHandler(OUTPUT_DIR / 'ado_scan.log', encoding='utf-8'),
                                  logging.StreamHandler()])
    start = time.monotonic()
    today = datetime.now(timezone.utc)
    since = today - relativedelta(months=MONTHS)
    LOG.info('СТАРТ. API %s, период создания PR: %s — %s, лимит файлов: %s, RPS: %s, workers: %s',
             API_VERSION, since.date(), today.date(), MAX_CHANGED_FILES, RPS, CONCURRENCY)
    auth = aiohttp.BasicAuth('', ADO_PAT)
    timeout = aiohttp.ClientTimeout(total=90)
    async with aiohttp.ClientSession(auth=auth, timeout=timeout, trust_env=True) as session:
        api = ADO(session)
        prs = await api.list_prs(since, today)
        # Extra safeguard if server's time filter behaves unexpectedly.
        prs = [p for p in prs if since <= datetime.fromisoformat(p['creationDate'].replace('Z', '+00:00')) <= today]
        LOG.info('Всего PR для анализа: %d', len(prs))
        queue = asyncio.Queue()
        for pr in prs:
            queue.put_nowait(pr)
        counters = {'found': 0, 'skipped': 0, 'error': 0, 'no_match': 0}
        found = []
        skipped = []
        errors = []
        not_matching = []
        progress = tqdm(total=len(prs), desc='Проверка PR', unit='PR', dynamic_ncols=True)

        async def worker():
            while True:
                try:
                    pr = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                try:
                    status, data = await inspect_pr(api, pr)
                    counters[status] += 1
                    if status == 'found':
                        found.append(data)
                        LOG.info('НАЙДЕН PR #%s: %s | файлов: %d', data['pr_id'], data['url'], len(data['matches']))
                    elif status == 'skipped':
                        skipped.append(data)
                        LOG.info('ПРОПУСК PR #%s: %s', data['pr_id'], data['reason'])
                    elif status == 'error':
                        errors.append(data)
                    elif status == 'no_match':
                        not_matching.append(data)
                    progress.update(1)
                    progress.set_postfix(found=counters['found'], skipped=counters['skipped'], errors=counters['error'], requests=api.n_requests)
                finally:
                    queue.task_done()

        try:
            await asyncio.gather(*(worker() for _ in range(CONCURRENCY)))
        finally:
            progress.close()

        # One PR per row, even if there are multiple large OpenAPI JSON files.
        rows = []
        for pr in sorted(found, key=lambda x: x['pr_id']):
            largest = max(pr['matches'], key=lambda m: m['fields'])
            rows.append({**{k: pr.get(k, '') for k in ('pr_id', 'url', 'title', 'created', 'status')},
                         'max_fields': largest['fields'], 'document_total_fields': largest['document_total_fields'],
                         'operation_count': largest['operation_count'], 'request_fields': largest['request_fields'],
                         'response_fields': largest['response_fields'], 'operation': largest['operation'],
                         'files': ' | '.join(x['file'] for x in pr['matches']),
                         'large_files_count': len(pr['matches'])})
        columns = ['pr_id', 'url', 'title', 'created', 'status', 'max_fields', 'document_total_fields', 'operation_count',
                   'request_fields', 'response_fields', 'operation', 'large_files_count', 'files']
        write_csv(OUTPUT_DIR / 'large_openapi_prs.csv', rows, columns)
        write_csv(OUTPUT_DIR / 'skipped_prs.csv', skipped, ['pr_id', 'url', 'reason', 'title', 'created', 'status'])
        write_csv(OUTPUT_DIR / 'errors_prs.csv', errors, ['pr_id', 'url', 'reason', 'title', 'created', 'status'])
        write_csv(OUTPUT_DIR / 'non_matching_prs.csv', not_matching, ['pr_id', 'url', 'reason', 'title', 'created', 'status'])
        summary = {'count_mode': COUNT_MODE, 'threshold': MIN_FIELDS, 'period_from': since.isoformat(), 'period_to': today.isoformat(),
                   'total_prs': len(prs), 'matching_prs': counters['found'],
                   'skipped_more_than_3_files': counters['skipped'],
                   'errors': counters['error'], 'checked_without_match': counters['no_match'],
                   'http_requests': api.n_requests, 'duration_seconds': round(time.monotonic() - start, 1),
                   'complete': counters['error'] == 0 and counters['skipped'] == 0,
                   'note': 'Skipped PRs were deliberately not analyzed; matching count is a lower bound.'}
        (OUTPUT_DIR / 'summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
        LOG.info('ГОТОВО. Найдено PR: %d; пропущено (>3 файлов): %d; ошибок: %d; HTTP: %d; время: %.1f сек',
                 counters['found'], counters['skipped'], counters['error'], api.n_requests, summary['duration_seconds'])
        LOG.info('Результаты: %s', OUTPUT_DIR.resolve())


if __name__ == '__main__':
    asyncio.run(main())