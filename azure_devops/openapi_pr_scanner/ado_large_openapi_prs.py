#!/usr/bin/env python3
"""Find Azure DevOps PRs changing large OpenAPI JSON contracts using local files."""
import argparse
import asyncio
import base64
import csv
import hashlib
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
from dotenv import load_dotenv

load_dotenv()

# ---------- Configuration (environment variables) ----------
ADO_ORG = os.environ.get('ADO_ORG', '').strip()
ADO_PROJECT = os.environ.get('ADO_PROJECT', '').strip()
ADO_REPO = os.environ.get('ADO_REPO', '').strip()  # ID or repo name
TFS_TOKEN = os.environ.get('TFS_TOKEN', '').strip()
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
API_VERSION = os.getenv('ADO_API_VERSION', '6.1-preview.1').strip()
MAX_RETRIES = 5
MAX_BLOB_BYTES = 3_000_000
LOCAL_REPO_PATH = Path(os.getenv('LOCAL_REPO_PATH', '')).expanduser()

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

    async def get_pr(self, pr_id):
        return await self.get(f'/pullrequests/{pr_id}')

    async def all_changes(self, pr_id, iteration):
        entries = []
        skip = 0
        while True:
            batch = await self.get(f'/pullRequests/{pr_id}/iterations/{iteration}/changes', {
                '$compareTo': 0, '$top': 100, '$skip': skip,
            })
            page = batch.get('changeEntries') or []
            entries.extend(page)
            next_skip = batch.get('nextSkip')
            if next_skip is not None and int(next_skip) > skip:
                skip = int(next_skip)
            elif len(page) == 100:
                skip += len(page)
            else:
                break
            if not page:
                break
        return entries

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
    """Count nested properties recursively, including arrays and schema composition.

    Each declared property counts once; nested properties count additionally.
    Local refs are expanded at every use, with recursion-cycle protection.
    For allOf, branches are combined (duplicate paths are counted once);
    for oneOf/anyOf the largest branch is selected.
    """
    active = active or frozenset()
    if not isinstance(schema, dict):
        return 0

    def paths(node, stack, prefix=()):
        if not isinstance(node, dict):
            return set()
        # Support $ref alongside sibling keywords (OpenAPI 3.1 / JSON Schema).
        result = set()
        ref = node.get('$ref')
        if isinstance(ref, str):
            if ref not in stack:
                target = resolve_ref(spec, ref)
                if target is None:
                    LOG.warning('Unresolved schema reference: %s', ref)
                else:
                    result.update(paths(target, stack | {ref}, prefix))
        props = node.get('properties')
        if isinstance(props, dict):
            for name, child in props.items():
                field_path = prefix + (str(name),)
                result.add(field_path)
                result.update(paths(child, stack, field_path))
        for branch in node.get('allOf') or []:
            result.update(paths(branch, stack, prefix))
        for key in ('oneOf', 'anyOf'):
            choices = [paths(branch, stack, prefix) for branch in (node.get(key) or [])]
            if choices:
                result.update(max(choices, key=len))
        # Array elements belong to the same structural path (not one per item).
        result.update(paths(node.get('items'), stack, prefix))
        # additionalProperties represents a map of potentially many keys;
        # only its statically declared nested schema fields are counted.
        result.update(paths(node.get('additionalProperties'), stack, prefix))
        return result

    return len(paths(schema, active))

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
    # Every JSON file in this repository is a contract. Do not require
    # an explicit OpenAPI/Swagger version marker in its root object.
    if not isinstance(spec, dict):
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

# Local repository is the ADO repository containing contracts, NOT this script's GitHub repo.
# Store normalized relative paths only; never let ADO paths escape LOCAL_REPO_PATH.
def normalized_ado_path(path):
    from pathlib import PurePosixPath
    if not isinstance(path, str) or not path.startswith('/') or '\\' in path:
        return None
    parts = PurePosixPath(path).parts[1:]
    if not parts or any(p in ('.', '..', '') for p in parts):
        return None
    return tuple(parts)


class LocalContracts:
    def __init__(self, root):
        self.root = root.resolve(strict=True)
        if not self.root.is_dir():
            raise ValueError(f'LOCAL_REPO_PATH не каталог: {self.root}')
        self.index = {}
        self.cache = {}
        # Index only JSON files. Sorted traversal keeps resolution reproducible.
        for path in sorted(self.root.rglob('*')):
            if not path.is_file() or path.suffix.lower() != '.json':
                continue
            resolved = path.resolve()
            if not resolved.is_relative_to(self.root):
                continue  # Do not traverse symlinks pointing outside the repo.
            relative = resolved.relative_to(self.root)
            self.index.setdefault(tuple(part.casefold() for part in relative.parts), []).append(resolved)
        LOG.info('Локальных JSON-файлов: %d (корень: %s)', sum(map(len, self.index.values())), self.root)

    def find(self, ado_path):
        parts = normalized_ado_path(ado_path)
        if parts is None:
            raise ValueError(f'Недопустимый путь ADO: {ado_path!r}')
        keys = tuple(p.casefold() for p in parts)
        # First exact repo-relative match, then reverse-search longest path suffix.
        # Never silently choose an ambiguous filename.
        for n in range(len(keys), 0, -1):
            suffix = keys[-n:]
            matches = [p for k, group in self.index.items() if len(k) >= n and k[-n:] == suffix for p in group]
            if len(matches) == 1:
                return matches[0]
            if len(matches) > 1:
                raise ValueError(f'Неоднозначный локальный путь {ado_path}: {len(matches)} совпадений по {"/".join(suffix)}')
        raise FileNotFoundError(f'Не найден локальный JSON: {ado_path}')

    def analyze(self, ado_path):
        local = self.find(ado_path)
        if local not in self.cache:
            try:
                spec = json.loads(local.read_text(encoding='utf-8-sig'))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ValueError(f'Некорректный JSON в {local}: {exc}') from exc
            self.cache[local] = (spec, analyze_openapi(spec))
        spec, stats = self.cache[local]
        return local, spec, stats


def pr_metadata(api, pr):
    return {'pr_id': pr['pullRequestId'], 'url': api.pr_link(pr),
            'title': pr.get('title', ''), 'created': pr.get('creationDate', ''),
            'status': pr.get('status', '')}


async def collect_pr_files(api, pr, ignore_file_limit=False):
    base = pr_metadata(api, pr)
    try:
        iteration = await api.last_iteration_id(pr['pullRequestId'])
        if iteration is None:
            return 'error', {**base, 'reason': 'Нет итераций'}
        if ignore_file_limit:
            entries = await api.all_changes(pr['pullRequestId'], iteration)
            more = False
        else:
            response = await api.changes_first_four(pr['pullRequestId'], iteration)
            entries = response.get('changeEntries') or []
            more = bool(response.get('nextSkip', 0))
        if not ignore_file_limit and (len(entries) > MAX_CHANGED_FILES or more):
            return 'skipped', {**base, 'reason': f'Более {MAX_CHANGED_FILES} изменённых файлов'}
        paths = []
        for entry in entries:
            change_type = str(entry.get('changeType', '')).lower()
            if not any(word in change_type for word in ('add', 'edit', 'rename', 'undelete')):
                continue
            item = entry.get('item') or {}
            file_path = item.get('path', '')
            if not item.get('isFolder') and isinstance(file_path, str) and file_path.lower().endswith('.json'):
                if file_path not in paths:
                    paths.append(file_path)
        return 'collected', {**base, 'files_to_check': paths}
    except Exception as exc:
        LOG.error('PR #%s: не удалось собрать пути: %s', pr['pullRequestId'], exc)
        return 'error', {**base, 'reason': str(exc)}


def evaluate_pr(collected, local, verbose=False):
    paths = collected['files_to_check']
    if not paths:
        return 'no_match', {**collected, 'reason': 'Нет добавленных/изменённых JSON-файлов'}
    matches = []
    with_operations = 0
    errors = []
    for path in paths:
        try:
            local_path, spec, stats = local.analyze(path)
            if verbose:
                LOG.info('ФАЙЛ %s -> %s', path, local_path)
                for api_path, raw in (spec.get('paths') or {}).items():
                    item = dereference(raw, spec)
                    for verb in ('get', 'post', 'put', 'patch', 'delete', 'options', 'head', 'trace'):
                        op = item.get(verb)
                        if isinstance(op, dict):
                            req, res = request_count(op, item, spec), response_count(op, spec)
                            LOG.info('  %s %s | REQUEST=%d RESPONSE=%d TOTAL=%d', verb.upper(), api_path, req, res, req + res)
            if stats:
                with_operations += 1
                score = stats['document_total_fields'] if COUNT_MODE == 'document' else stats['fields']
                if score >= MIN_FIELDS:
                    matches.append({'file': path, 'local_file': str(local_path), **stats})
            elif verbose:
                LOG.warning('Нет HTTP-операций в %s', path)
        except (OSError, ValueError, TypeError, AttributeError) as exc:
            errors.append(f'{path}: {exc}')
            LOG.error('PR #%s: %s', collected['pr_id'], errors[-1])
    if errors:
        return 'error', {**collected, 'reason': ' | '.join(errors),
                         'partial_matches': matches}
    if matches:
        return 'found', {**collected, 'matches': matches}
    return 'no_match', {**collected, 'reason':
                        'В JSON нет HTTP-операций' if not with_operations else 'Порог полей не достигнут'}


async def diagnose_pr(api, pr_id, local, ignore_file_limit=False):
    pr = await api.get_pr(pr_id)
    LOG.info('ДИАГНОСТИКА PR #%s | %s', pr_id, api.pr_link(pr))
    status, data = await collect_pr_files(api, pr, ignore_file_limit)
    if status != 'collected':
        LOG.warning('Результат: %s: %s', status, data.get('reason'))
        return 3 if status == 'skipped' else 2
    for path in data['files_to_check']:
        LOG.info('ADO JSON: %s', path)
    status, result = evaluate_pr(data, local, verbose=True)
    LOG.info('ИТОГ PR #%s: %s %s', pr_id, status, result.get('reason', ''))
    return 0 if status == 'found' else 2 if status == 'error' else 1


def write_csv(path, rows, columns):
    with path.open('w', newline='', encoding='utf-8-sig') as fh:
        writer = csv.DictWriter(fh, fieldnames=columns, extrasaction='ignore', delimiter=';')
        writer.writeheader()
        writer.writerows(rows)


async def main():
    parser = argparse.ArgumentParser(description='Azure DevOps OpenAPI PR scanner')
    parser.add_argument('--pr-id', type=int, help='Диагностика одного PR по ID')
    parser.add_argument('--ignore-file-limit', action='store_true', help='Только с --pr-id: исследовать PR с >3 изменениями')
    args = parser.parse_args()
    if args.ignore_file_limit and args.pr_id is None:
        parser.error('--ignore-file-limit используется только с --pr-id')
    if args.pr_id is not None and args.pr_id <= 0:
        parser.error('--pr-id должен быть положительным')
    if not all((ADO_PROJECT, ADO_REPO, TFS_TOKEN)) or not (ADO_ORG or ADO_COLLECTION_URL):
        raise SystemExit('Укажите ADO_PROJECT, ADO_REPO, TFS_TOKEN и одно из ADO_ORG/ADO_COLLECTION_URL')
    if COUNT_MODE not in ('operation', 'document'):
        raise SystemExit('COUNT_MODE должен быть operation или document')
    if CONCURRENCY < 1 or RPS < 1 or MAX_CHANGED_FILES < 1:
        raise SystemExit('CONCURRENCY, RPS, MAX_CHANGED_FILES должны быть >= 1')
    if not os.getenv('LOCAL_REPO_PATH', '').strip():
        raise SystemExit('Укажите LOCAL_REPO_PATH — путь к локальному клону ADO-репозитория с JSON')
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)-7s | %(message)s',
                        handlers=[logging.FileHandler(OUTPUT_DIR / 'ado_scan.log', encoding='utf-8'),
                                  logging.StreamHandler()])
    try:
        local = LocalContracts(LOCAL_REPO_PATH)
    except (OSError, ValueError) as exc:
        raise SystemExit(f'Ошибка локального репозитория: {exc}') from exc
    start = time.monotonic()
    today = datetime.now(timezone.utc)
    since = today - relativedelta(months=MONTHS)
    LOG.info('СТАРТ. API %s, период создания PR: %s — %s, лимит файлов: %s, RPS: %s, workers: %s',
             API_VERSION, since.date(), today.date(), MAX_CHANGED_FILES, RPS, CONCURRENCY)
    auth = aiohttp.BasicAuth('', TFS_TOKEN)
    timeout = aiohttp.ClientTimeout(total=90)
    async with aiohttp.ClientSession(auth=auth, timeout=timeout, trust_env=True) as session:
        api = ADO(session)
        if args.pr_id is not None:
            try:
                verdict = await diagnose_pr(api, args.pr_id, local, args.ignore_file_limit)
                LOG.info('Диагностика завершена; HTTP-запросов: %d; длительность: %.1f сек',
                         api.n_requests, time.monotonic() - start)
                return verdict
            except Exception as exc:
                LOG.exception('Диагностика завершилась ошибкой: %s', exc)
                return 2
        prs = await api.list_prs(since, today)
        # Extra safeguard if server's time filter behaves unexpectedly.
        prs = [p for p in prs if since <= datetime.fromisoformat(p['creationDate'].replace('Z', '+00:00')) <= today]
        LOG.info('Всего PR для анализа: %d', len(prs))
        queue = asyncio.Queue()
        for pr in prs:
            queue.put_nowait(pr)
        counters = {'found': 0, 'skipped': 0, 'error': 0, 'no_match': 0}
        found = []
        collected = []
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
                    status, data = await collect_pr_files(api, pr)
                    if status != 'collected':
                        counters[status] += 1
                    if status == 'collected':
                        collected.append(data)
                    elif status == 'skipped':
                        skipped.append(data)
                        LOG.info('ПРОПУСК PR #%s: %s', data['pr_id'], data['reason'])
                    elif status == 'error':
                        errors.append(data)
                    elif status == 'no_match':
                        not_matching.append(data)
                    progress.update(1)
                    progress.set_postfix(collected=len(collected), skipped=counters['skipped'], errors=counters['error'], requests=api.n_requests)
                finally:
                    queue.task_done()

        try:
            await asyncio.gather(*(worker() for _ in range(CONCURRENCY)))
        finally:
            progress.close()

        # Stage 2: no further HTTP calls; sequential local resolution/read, with shared cache.
        LOG.info('Сбор завершён: %d PR. Начинаем последовательную локальную проверку.', len(collected))
        for item in tqdm(sorted(collected, key=lambda x: x['pr_id'], reverse=True), desc='Локальные JSON', unit='PR'):
            status, data = evaluate_pr(item, local)
            counters[status] += 1
            if status == 'found':
                found.append(data)
            elif status == 'error':
                errors.append(data)
            else:
                not_matching.append(data)

        # One PR per row, even if there are multiple large OpenAPI JSON files.
        rows = []
        for pr in sorted(found, key=lambda x: x['pr_id']):
            largest = max(pr['matches'], key=lambda m: m['fields'])
            rows.append({**{k: pr.get(k, '') for k in ('pr_id', 'url', 'title', 'created', 'status')},
                         'max_fields': largest['fields'], 'document_total_fields': largest['document_total_fields'],
                         'operation_count': largest['operation_count'], 'request_fields': largest['request_fields'],
                         'response_fields': largest['response_fields'], 'operation': largest['operation'],
                         'files': ' | '.join(x['file'] for x in pr['matches']),
                         'local_files': ' | '.join(x['local_file'] for x in pr['matches']),
                         'large_files_count': len(pr['matches'])})
        columns = ['pr_id', 'url', 'title', 'created', 'status', 'max_fields', 'document_total_fields', 'operation_count',
                   'request_fields', 'response_fields', 'operation', 'large_files_count', 'files', 'local_files']
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
                   'content_source': 'local_working_tree', 'local_repo_path': str(local.root),
                   'note': 'Uses current local working tree, not historical PR versions. Skipped/error PRs make match count a lower bound.'}
        (OUTPUT_DIR / 'summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
        LOG.info('ГОТОВО. Найдено PR: %d; пропущено (>3 файлов): %d; ошибок: %d; HTTP: %d; время: %.1f сек',
                 counters['found'], counters['skipped'], counters['error'], api.n_requests, summary['duration_seconds'])
        LOG.info('Результаты: %s', OUTPUT_DIR.resolve())


if __name__ == '__main__':
    raise SystemExit(asyncio.run(main()) or 0)