"""
Visualization — небольшой помощник для построения типовых графиков.

Поддерживает работу как с обычными массивами, так и с названиями
столбцов DataFrame, переданного при создании объекта.
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


class Visualization:
    """Набор простых методов для быстрой визуализации данных."""

    def __init__(self, data=None, figsize=(6, 4)):
        self.data = data
        self.figsize = figsize

    # -------------------- внутренние методы --------------------

    def _get_data(self, source):
        """Возвращает столбец DataFrame либо переданное значение."""
        if not isinstance(source, str):
            return source

        if self.data is None:
            raise ValueError(
                f'Нельзя получить столбец "{source}": DataFrame не был передан.'
            )

        if source not in self.data.columns:
            raise KeyError(f'Столбец "{source}" отсутствует в DataFrame.')

        return self.data[source]

    def _as_array(self, source):
        """Преобразует источник данных в numpy-массив."""
        value = self._get_data(source)

        if isinstance(value, (pd.Series, pd.DataFrame)):
            value = value.to_numpy()

        return np.asarray(value)

    @staticmethod
    def _column_name(value):
        return value if isinstance(value, str) else None

    def _create_plot(self, *, title=None, xlabel=None, ylabel=None):
        """Создаёт фигуру и оси и сразу применяет подписи."""
        fig, ax = plt.subplots(figsize=self.figsize, layout="constrained")

        labels = {
            "title": title,
            "xlabel": xlabel,
            "ylabel": ylabel,
        }

        if labels["title"]:
            ax.set_title(labels["title"])
        if labels["xlabel"]:
            ax.set_xlabel(labels["xlabel"])
        if labels["ylabel"]:
            ax.set_ylabel(labels["ylabel"])

        return fig, ax

    @staticmethod
    def _complete(fig, show):
        if show:
            plt.show()
        return fig

    def _frequency_table(self, source, order=None):
        """Возвращает категории и число их появлений."""
        values = self._as_array(source)
        labels, counts = np.unique(values, return_counts=True)

        if order is None:
            return labels.astype(str), counts

        frequencies = {
            str(label): count
            for label, count in zip(labels, counts)
        }

        ordered_labels = np.asarray(order, dtype=str)
        ordered_counts = np.asarray(
            [frequencies.get(label, 0) for label in ordered_labels]
        )

        return ordered_labels, ordered_counts

    # -------------------- графики --------------------

    def scatter(
        self,
        x,
        y,
        hue=None,
        title=None,
        xlabel=None,
        ylabel=None,
        alpha=0.6,
        show=True,
        **kwargs,
    ):
        """Строит диаграмму рассеяния."""
        x_name = self._column_name(x)
        y_name = self._column_name(y)

        default_title = (
            f"{x_name} vs {y_name}"
            if x_name is not None and y_name is not None
            else None
        )

        fig, ax = self._create_plot(
            title=title or default_title,
            xlabel=xlabel or x_name,
            ylabel=ylabel or y_name,
        )

        can_use_hue = (
            hue is not None
            and self.data is not None
            and x_name is not None
            and y_name is not None
        )

        if can_use_hue:
            sns.scatterplot(
                data=self.data,
                x=x_name,
                y=y_name,
                hue=hue,
                alpha=alpha,
                ax=ax,
                **kwargs,
            )
        else:
            ax.scatter(
                self._as_array(x),
                self._as_array(y),
                alpha=alpha,
                **kwargs,
            )

        return self._complete(fig, show)

    def bar(
        self,
        x,
        y=None,
        title=None,
        xlabel=None,
        ylabel=None,
        show=True,
        **kwargs,
    ):
        """Строит столбчатую диаграмму или частоты категорий."""
        if y is None:
            labels, counts = self._frequency_table(x)

            fig, ax = self._create_plot(
                title=title or (
                    f"Частоты: {x}" if isinstance(x, str) else "Частоты"
                ),
                xlabel=xlabel or (
                    x if isinstance(x, str) else "категория"
                ),
                ylabel=ylabel or "количество",
            )

            ax.bar(labels, counts, **kwargs)
            return self._complete(fig, show)

        x_values = self._as_array(x).astype(str)
        y_values = self._as_array(y)

        fig, ax = self._create_plot(
            title=title,
            xlabel=xlabel or self._column_name(x),
            ylabel=ylabel or self._column_name(y),
        )

        ax.bar(x_values, y_values, **kwargs)
        return self._complete(fig, show)

    def line(
        self,
        x,
        y,
        title=None,
        xlabel=None,
        ylabel=None,
        marker="o",
        show=True,
        **kwargs,
    ):
        """Строит линейный график."""
        x_name = self._column_name(x)
        y_name = self._column_name(y)

        generated_title = (
            f"{y_name} по {x_name}"
            if x_name is not None and y_name is not None
            else None
        )

        fig, ax = self._create_plot(
            title=title or generated_title,
            xlabel=xlabel or x_name,
            ylabel=ylabel or y_name,
        )

        ax.plot(
            self._as_array(x),
            self._as_array(y),
            marker=marker,
            **kwargs,
        )

        return self._complete(fig, show)

    def hist(
        self,
        x,
        bins=20,
        title=None,
        xlabel=None,
        ylabel="частота",
        show_stats=True,
        show=True,
        **kwargs,
    ):
        """Строит гистограмму и при необходимости отмечает среднее и медиану."""
        values = self._as_array(x)
        frequencies, borders = np.histogram(values, bins=bins)

        fig, ax = self._create_plot(
            title=title or (
                f"Гистограмма: {x}" if isinstance(x, str) else "Гистограмма"
            ),
            xlabel=xlabel or self._column_name(x),
            ylabel=ylabel,
        )

        ax.bar(
            borders[:-1],
            frequencies,
            width=np.diff(borders),
            align="edge",
            edgecolor="white",
            **kwargs,
        )

        if show_stats:
            statistics = {
                "mean": np.mean(values),
                "median": np.median(values),
            }

            ax.axvline(
                statistics["mean"],
                linestyle="--",
                label=f'mean={statistics["mean"]:.2f}',
            )
            ax.axvline(
                statistics["median"],
                linestyle="--",
                label=f'median={statistics["median"]:.2f}',
            )
            ax.legend()

        return self._complete(fig, show)

    def box(self, x=None, y=None, title=None, show=True, **kwargs):
        """Строит boxplot для одного признака либо по категориям."""
        grouped = (
            x is not None
            and y is not None
            and self.data is not None
            and isinstance(x, str)
            and isinstance(y, str)
        )

        if grouped:
            fig, ax = self._create_plot(title=title or f"{y} по {x}")
            sns.boxplot(
                data=self.data,
                x=x,
                y=y,
                ax=ax,
                **kwargs,
            )
            return self._complete(fig, show)

        column = y if y is not None else x
        column_name = self._column_name(column)

        fig, ax = self._create_plot(
            title=title or (
                f"Boxplot: {column_name}"
                if column_name is not None
                else "Boxplot"
            ),
            ylabel=column_name,
        )

        ax.boxplot(self._as_array(column), **kwargs)
        return self._complete(fig, show)

    def heatmap(
        self,
        cols=None,
        title="Корреляция",
        annot=True,
        show=True,
        **kwargs,
    ):
        """Строит тепловую карту корреляций числовых признаков."""
        if self.data is None:
            raise ValueError("Для heatmap необходимо передать DataFrame в data.")

        selected_columns = (
            list(self.data.select_dtypes(include="number").columns)
            if cols is None
            else list(cols)
        )

        matrix = self.data[selected_columns].to_numpy()
        correlation = np.corrcoef(matrix, rowvar=False)

        fig, ax = self._create_plot(title=title)

        sns.heatmap(
            correlation,
            annot=annot,
            fmt=".2f",
            cmap="coolwarm",
            center=0,
            xticklabels=selected_columns,
            yticklabels=selected_columns,
            square=True,
            ax=ax,
            **kwargs,
        )

        return self._complete(fig, show)

    def count(self, x, order=None, title=None, show=True, **kwargs):
        """Показывает количество объектов в каждой категории."""
        labels, counts = self._frequency_table(x, order=order)

        fig, ax = self._create_plot(
            title=title or (
                f"Частоты: {x}" if isinstance(x, str) else "Частоты"
            ),
            xlabel=self._column_name(x) or "категория",
            ylabel="количество",
        )

        ax.bar(labels, counts, **kwargs)
        return self._complete(fig, show)

    def pie(self, x, title=None, show=True, **kwargs):
        """Строит круговую диаграмму по частотам категорий."""
        labels, counts = self._frequency_table(x)

        fig, ax = self._create_plot(
            title=title or (
                f"Доли: {x}" if isinstance(x, str) else "Доли"
            )
        )

        ax.pie(
            counts,
            labels=labels,
            autopct="%1.1f%%",
            **kwargs,
        )
        ax.set_ylabel("")

        return self._complete(fig, show)


if __name__ == "__main__":
    dataset = pd.read_csv(
        "Boston2_with_vars.csv",
        sep=";",
        decimal=",",
    )

    charts = Visualization(dataset)

    charts.count("PRICE", order=["LOW", "MEDIUM", "HIGH"])
    charts.scatter("RM", "LSTAT", hue="PRICE")
    charts.hist("RM", bins=20)
    charts.box(x="PRICE", y="RM")

    first_rows = dataset["RM"].head(50).to_numpy()
    charts.line(
        np.arange(first_rows.size),
        first_rows,
        title="RM: первые 50 наблюдений",
        xlabel="индекс",
        ylabel="RM",
    )

    charts.heatmap(
        cols=["CRIM", "RM", "AGE", "DIS", "TAX", "PTRATIO", "LSTAT"]
    )
    charts.pie("PRICE")
