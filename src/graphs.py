import altair as alt
import polars as pl

from .data.data_process import DataIndex


class DataGraph(DataIndex):
    def __init__(
        self,
        saving_dir: str = "data/",
        database_file: str = "data.ddb",
        log_file: str = "data_process.log",
    ):
        super().__init__(saving_dir, database_file, log_file)

    def format_money(self, val):
        abs_val = abs(val)
        sign = "-" if val < 0 else ""

        if abs_val >= 1e9:
            return f"{sign}${abs_val / 1e9:.1f}B"
        elif abs_val >= 1e6:
            return f"{sign}${abs_val / 1e6:.1f}M"
        elif abs_val >= 1e3:
            return f"{sign}${abs_val / 1e3:.1f}K"
        else:
            return f"{sign}${abs_val:.0f}"

    def create_spending_by_category_graph(
        self, year: int, quarter: int, month: int, type: str, category: str
    ):
        df, columns = self.process_awards_by_category(
            year, quarter, month, type, category
        )
        grouped_pd = df.to_pandas()
        grouped_pd["formatted_text"] = grouped_pd["federal_action_obligation"].apply(
            self.format_money
        )

        chart = (
            alt.Chart(grouped_pd)
            .mark_bar()
            .encode(
                y=alt.Y(f"{category}:N", title="", sort="-x"),
                x=alt.X(
                    "federal_action_obligation:Q",
                    title="",
                    scale=alt.Scale(type="sqrt"),
                    axis=None,
                ),
                tooltip=[
                    alt.Tooltip("federal_action_obligation:Q", title="Periodo"),
                    alt.Tooltip(f"{category}:N", title=category),
                ],
            )
        )

        text = (
            alt.Chart(grouped_pd)
            .mark_text(
                baseline="middle",
                align=alt.ExprRef(
                    "datum.federal_action_obligation < 0 ? 'right' : 'left'"
                ),
                dx=alt.ExprRef("datum.federal_action_obligation < 0 ? -3 : 3"),
            )
            .encode(
                y=alt.Y(f"{category}:N", sort="-x"),
                x=alt.X("federal_action_obligation:Q"),
                text="formatted_text:N",
            )
        )

        data_chart = (
            (chart + text)
            .properties(
                width="container",
            )
            .configure_view(fill="#e6f7ff")
            .configure_axis(gridColor="white", grid=True)
        )
        return data_chart, columns

    def create_secter_graph(self, type: str, secter: str):
        df, agency_list = self.process_awards_by_secter(type, secter)

        grouped_pd = df.to_pandas()
        grouped_pd["formatted_text"] = grouped_pd["federal_action_obligation"].apply(
            self.format_money
        )

        if type == "monthly":
            period = "parsed_period"
            sort_expr = grouped_pd["parsed_period"].tolist()
        else:
            period = "time_period"
            sort_expr = "x"

        if type == "monthly":
            num_points = len(grouped_pd[period].unique())
            if num_points < 84:
                chart_width = "container"
            else:
                chart_width = max(600, num_points * 15)
        else:
            chart_width = "container"

        data_chart = (
            alt.Chart(grouped_pd)
            .mark_line()
            .encode(
                x=alt.X(f"{period}:O", title="", sort=sort_expr),
                y=alt.Y(
                    "federal_action_obligation:Q",
                    title="",
                ),
                tooltip=[
                    alt.Tooltip(f"{period}:O", title="Periodo"),
                    alt.Tooltip(
                        f"federal_action_obligation:Q",
                        title="federal_action_obligation",
                    ),
                ],
            )
            .properties(
                width=chart_width,
            )
            .configure_view(fill="#e6f7ff")
            .configure_axis(gridColor="white", grid=True)
        )

        return data_chart, agency_list

    def _detect_unidad_y_formato(self, metric: str):
        metric_lower = metric.lower()
        if metric_lower.endswith("_mkwh"):
            return ("kWh", ".0f")
        if metric_lower.endswith("_mw"):
            return ("MW", ".0f")
        if metric_lower.endswith("_mdollar"):
            return ("Millones USD", ".2~f")
        if "cent_kwh" in metric_lower or metric_lower.endswith("_centkwh"):
            return ("¢/kWh", ".2f")
        if metric_lower.endswith("_cent"):
            return ("¢", ".2f")
        if metric_lower.startswith("clientes_activos"):
            return ("# Clientes", "d")
        return (metric.replace("_", " ").capitalize(), ".2f")

    def create_energy_chart(self, period: str, metric: str):
        df_grouped, energy_metrics = self.process_energy_data(period, metric)

        y_title, y_format = self._detect_unidad_y_formato(metric)

        unique_periods = df_grouped["time_period"].unique().to_list()
        months = [
            "Jan",
            "Feb",
            "Mar",
            "Apr",
            "May",
            "Jun",
            "Jul",
            "Aug",
            "Sep",
            "Oct",
            "Nov",
            "Dec",
        ]

        if period.lower() == "monthly":

            def sort_key(tp):
                return (int(tp[:4]), months.index(tp[4:]) + 1)
        elif period.lower() == "quarterly":

            def sort_key(tp):
                yr, qr = tp.split("-")
                return (int(yr), int(qr[1]))
        else:

            def sort_key(tp):
                return int(tp)

        x_order = sorted(unique_periods, key=sort_key)

        mapping_df = pl.DataFrame(
            {
                "time_period": x_order,
                "_order": list(range(len(x_order))),
            }
        )

        df_sorted = (
            df_grouped.join(mapping_df, on="time_period", how="left")
            .sort("_order")
            .drop("_order")
        )

        if period == "monthly":
            tick_vals = x_order[::6]
        elif period == "quarterly":
            tick_vals = x_order[::3]
        else:
            tick_vals = x_order
        data = df_sorted.to_dicts()

        chart = (
            alt.Chart(alt.Data(values=data))
            .mark_line(point=True)
            .encode(
                x=alt.X(
                    "time_period:N",
                    title="Periodo",
                    sort=x_order,
                    axis=alt.Axis(labelAngle=-45, values=tick_vals),
                ),
                y=alt.Y(f"{metric}:Q", title=y_title, axis=alt.Axis(format=y_format)),
                tooltip=[
                    alt.Tooltip("time_period:N", title="Periodo"),
                    alt.Tooltip(f"{metric}:Q", title=y_title, format=y_format),
                ],
            )
            .properties(
                width="container",
                height=300,
                title=f"Evolución de {metric.replace('_', ' ')} ({period.capitalize()})",
            )
            .configure_view(fill="#e6f7ff")
            .configure_axis(grid=True, gridColor="white")
            .interactive(bind_y=False)
        )
        return chart, energy_metrics

    def create_indicators_graph(self, time_frame: str, column: str) -> alt.Chart:
        df = self.jp_indicator_data(time_frame)
        df = df.fill_null(0).fill_nan(0)

        exclude_columns = ["date", "month", "year", "quarter", "fiscal"]
        columns = [
            {"value": col, "label": col.replace("_", " ").capitalize()}
            for col in df.columns
            if col not in exclude_columns
        ]

        if time_frame == "fiscal":
            df = df.filter(pl.col("fiscal") < 2024)
        else:
            df = df.filter(pl.col("year") < 2025)

        if time_frame == "fiscal":
            frequency = "fiscal"
            df = df.sort(frequency)
        elif time_frame == "yearly":
            frequency = "year"
            df = df.sort(frequency)
        elif time_frame == "monthly":
            frequency = "year_month"
            df = df.with_columns(
                (
                    pl.col("year").cast(pl.Utf8)
                    + "-"
                    + pl.col("month").cast(pl.Utf8).str.zfill(2)
                ).alias(frequency)
            )
            df = df.sort(frequency)
        elif time_frame == "quarterly":
            frequency = "year_quarter"
            df = df.with_columns(
                (
                    pl.col("year").cast(pl.String)
                    + "-q"
                    + pl.col("quarter").cast(pl.String)
                ).alias(frequency)
            )
            df = df.sort(frequency)

        df = df.filter(pl.col(column) != 0)
        min_idx = df.select(pl.col(column).arg_min()).item()
        max_idx = df.select(pl.col(column).arg_max()).item()

        range_min = df[column][min_idx] - df[column][min_idx] * 0.2
        range_max = df[column][max_idx] + df[column][max_idx] * 0.2

        chart_width = "container"

        x_values = df.select(frequency).unique().to_series().to_list()

        if time_frame == "monthly":
            tick_vals = x_values[::6]
        elif time_frame == "quarterly":
            tick_vals = x_values[::3]
        else:
            tick_vals = x_values

        chart = (
            (
                alt.Chart(df)
                .mark_line()
                .encode(
                    x=alt.X(
                        f"{frequency}:N", title="", axis=alt.Axis(values=tick_vals)
                    ),
                    y=alt.Y(
                        f"{column}:Q",
                        title=f"",
                        scale=alt.Scale(domain=[range_min, range_max]),
                    ),
                    tooltip=[
                        alt.Tooltip(f"{frequency}:N", title="Periodo"),
                        alt.Tooltip(
                            f"{column}:Q",
                        ),
                    ],
                )
                .properties(
                    width=chart_width, padding={"top": 10, "bottom": 10, "left": 30}
                )
            )
            .configure_view(fill="#e6f7ff")
            .configure_axis(gridColor="white", grid=True)
        )

        return chart, columns

    def create_consumer_graph(
        self, time_frame: str, column: str, data_type: str
    ) -> alt.Chart:
        df = self.process_consumer_data(time_frame, data_type)
        df = df.fill_null(0).fill_nan(0)

        exclude_columns = ["date", "month", "year", "quarter", "fiscal"]

        columns = [
            {"value": col, "label": col.replace("_", " ").capitalize()}
            for col in df.columns
            if col not in exclude_columns
        ]

        if time_frame == "fiscal":
            frequency = "fiscal"
            df = df.sort(frequency)
        elif time_frame == "yearly":
            frequency = "year"
            df = df.sort(frequency)
        elif time_frame == "monthly":
            frequency = "year_month"
            df = df.with_columns(
                (
                    pl.col("year").cast(pl.Utf8)
                    + "-"
                    + pl.col("month").cast(pl.Utf8).str.zfill(2)
                ).alias(frequency)
            )
            df = df.sort(frequency)
        elif time_frame == "quarterly":
            frequency = "year_quarter"
            df = df.with_columns(
                (
                    pl.col("year").cast(pl.String)
                    + "-q"
                    + pl.col("quarter").cast(pl.String)
                ).alias(frequency)
            )
            df = df.sort(frequency)
        df = df.group_by(frequency).agg(pl.col(column).sum())

        chart_width = "container"

        x_values = df.select(frequency).unique().to_series().to_list()

        if time_frame == "monthly":
            tick_vals = x_values[::6]
        elif time_frame == "quarterly":
            tick_vals = x_values[::3]
        else:
            tick_vals = x_values

        chart = (
            (
                alt.Chart(df)
                .mark_line()
                .encode(
                    x=alt.X(
                        f"{frequency}:N", title="", axis=alt.Axis(values=tick_vals)
                    ),
                    y=alt.Y(f"{column}:Q", title=f""),
                    tooltip=[
                        alt.Tooltip(f"{frequency}:N", title="Periodo"),
                        alt.Tooltip(
                            f"{column}:Q",
                        ),
                    ],
                )
                .properties(
                    width=chart_width, padding={"top": 10, "bottom": 10, "left": 30}
                )
            )
            .configure_view(fill="#e6f7ff")
            .configure_axis(gridColor="white", grid=True)
        )

        return chart, columns
