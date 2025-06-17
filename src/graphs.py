import altair as alt
from .data.data_process import DataIndex
import pandas as pd
import numpy as np
import duckdb
import polars as pl
from .data.data_pull import DataPull


class DataGraph(DataIndex):
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
    
    def create_spending_by_category_graph(self, year: int, quarter: int, month: int, type: str, category: str):
        df, columns = self.process_awards_by_category(year, quarter, month, type, category)
        grouped_pd = df.to_pandas()
        grouped_pd['formatted_text'] = grouped_pd["federal_action_obligation"].apply(self.format_money)

        chart = alt.Chart(grouped_pd).mark_bar().encode(
            y=alt.Y(f'{category}:N', title="", sort='-x'),
            x=alt.X(
                'federal_action_obligation:Q', 
                title="",
                scale=alt.Scale(type='sqrt'),
                axis=None
            ),
            tooltip=[
                alt.Tooltip("federal_action_obligation:Q", title="Periodo"),
                alt.Tooltip(f"{category}:N", title=category)
            ]
        )

        text = alt.Chart(grouped_pd).mark_text(
            baseline='middle',
            align=alt.ExprRef("datum.federal_action_obligation < 0 ? 'right' : 'left'"),
            dx=alt.ExprRef("datum.federal_action_obligation < 0 ? -3 : 3")
        ).encode(
            y=alt.Y(f'{category}:N', sort='-x'),
            x=alt.X('federal_action_obligation:Q'),
            text='formatted_text:N'
        )

        data_chart = (chart + text).properties(
            width='container',
        ).configure_view(
            fill='#e6f7ff'
        ).configure_axis(
            gridColor='white',
            grid=True
        )                                            
        return data_chart, columns
    
    def create_secter_graph(self, type: str, secter: str):
        df, agency_list = self.process_awards_by_secter(type, secter)

        grouped_pd = df.to_pandas()
        grouped_pd['formatted_text'] = grouped_pd["federal_action_obligation"].apply(self.format_money)

        if type == 'monthly':
            period = "parsed_period"
            sort_expr = grouped_pd["parsed_period"].tolist()
        else:
            period = 'time_period'
            sort_expr = 'x'

        if type == 'monthly':
            num_points = len(grouped_pd[period].unique())
            if num_points < 84:
                chart_width = 'container'
            else:
                chart_width = max(600, num_points * 15)
        else:
            chart_width = 'container'

        data_chart = alt.Chart(grouped_pd).mark_line().encode(
            x=alt.X(f'{period}:O', title='', sort=sort_expr),
            y=alt.Y('federal_action_obligation:Q', title="",),
            tooltip=[
                alt.Tooltip(f"{period}:O", title="Periodo"),
                alt.Tooltip(f"federal_action_obligation:Q", title='federal_action_obligation')
            ]
        ).properties(
            width=chart_width,           
        ).configure_view(
            fill='#e6f7ff'
        ).configure_axis(
            gridColor='white',
            grid=True
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

    def create_energy_chart(self, period: str, metric: str) -> alt.Chart:
        dp = DataPull()
        dp.pull_energy_data()
        dp.insert_energy_data()
        conn = duckdb.connect(self.data_file)
        df = conn.execute("SELECT * FROM EnergyTable").fetchdf()

        df["fecha"] = pd.to_datetime(df["mes"], format="%m/%d/%Y", errors="coerce")
        df["anio"]    = df["fecha"].dt.year
        df["mes_num"] = df["fecha"].dt.month
        df["periodo_mensual"] = df["fecha"].dt.to_period("M").astype(str)
        df["periodo_trimestral"] = df["fecha"].dt.to_period("Q").astype(str)
        df["anio_fiscal"] = np.where(df["mes_num"] >= 7, df["anio"] + 1, df["anio"])

        periodo = period.lower()
        if periodo not in ["monthly", "quarterly", "yearly", "fiscal"]:
            raise ValueError("`period` debe ser uno de: 'monthly','quarterly','yearly','fiscal'.")

        if periodo == "monthly":
            grouped = (
                df[["periodo_mensual", metric]]
                  .groupby("periodo_mensual", as_index=False)[metric]
                  .sum()
            )
            grouped["fecha_plot"] = pd.to_datetime(grouped["periodo_mensual"] + "-01",
                                                   format="%Y-%m-%d")
            df_plot = grouped.rename(columns={
                "periodo_mensual": "periodo",
                "fecha_plot": "fecha"
            })

        elif periodo == "quarterly":
            grouped = (
                df[["periodo_trimestral", metric]]
                  .groupby("periodo_trimestral", as_index=False)[metric]
                  .sum()
            )
            grouped["fecha"] = grouped["periodo_trimestral"].apply(
                lambda s: pd.Period(s, freq="Q").start_time
            )
            df_plot = grouped.rename(columns={"periodo_trimestral": "periodo"})

        elif periodo == "yearly":
            grouped = (
                df[["anio", metric]]
                  .groupby("anio", as_index=False)[metric]
                  .sum()
            )
            grouped["fecha"] = pd.to_datetime(grouped["anio"].astype(str) + "-01-01",
                                              format="%Y-%m-%d")
            df_plot = grouped.rename(columns={"anio": "periodo"})

        else:
            grouped = (
                df[["anio_fiscal", metric]]
                  .groupby("anio_fiscal", as_index=False)[metric]
                  .sum()
            )
            grouped["fecha"] = grouped["anio_fiscal"].apply(
                lambda y: pd.to_datetime(f"{y-1}-07-01", format="%Y-%m-%d")
            )
            grouped["periodo"] = "FY " + grouped["anio_fiscal"].astype(str)
            df_plot = grouped.drop(columns=["anio_fiscal"])

        y_title, y_format = self._detect_unidad_y_formato(metric)

        if periodo == "monthly":
            x_encoding = alt.X(
                "fecha:T",
                title="Monthly",
                axis=alt.Axis(format="%Y-%m", tickCount="month", labelAngle=90),
                scale=alt.Scale(domain=[
                    df_plot["fecha"].max() - pd.DateOffset(years=5),
                    df_plot["fecha"].max()
                ])
            )

        elif periodo == "quarterly":
            x_encoding = alt.X(
                "fecha:T",
                timeUnit="yearquarter",
                title="Quarterly",
                axis=alt.Axis(labelAngle=90)
            )

        elif periodo == "yearly":
            x_encoding = alt.X(
                "fecha:T",
                timeUnit="year",
                title="Yearly",
                axis=alt.Axis(format="%Y", tickCount="year", labelAngle=90)
            )

        else: 
            x_encoding = alt.X(
                "fecha:T",
                title="Fiscal Year",
                axis=alt.Axis(format=" %Y", tickCount="year", labelAngle=90)
            )

        chart = (
            alt.Chart(df_plot)
            .mark_line(point=True, color="#1f77b4")
            .encode(
                x=x_encoding,
                y=alt.Y(f"{metric}:Q",
                        title=y_title,
                        axis=alt.Axis(format=y_format)),
                tooltip=[
                    alt.Tooltip("periodo:N", title="Periodo"),
                    alt.Tooltip(f"{metric}:Q", title=y_title, format=y_format)
                ]
            )
            .properties(
                width="container",
                height=300,
                title=f"Evolución de {metric.replace('_',' ')} ({period.capitalize()})"
            )
            .configure_view(fill="#e6f7ff")
            .configure_axis(gridColor="white", grid=True)
            .interactive(bind_y=False)
        )

        return chart
    
    def create_indicators_graph(self, time_frame: str, column: str) -> alt.Chart:
        df = self.jp_indicator_data(time_frame)
        df = df.fill_null(0).fill_nan(0)

        exclude_columns = ["date", "month", "year", "quarter", "fiscal"]
        columns = [
            {"value": col, "label": col.replace("_", " ").capitalize()}
            for col in df.columns
            if col not in exclude_columns
        ]

        if time_frame == 'fiscal':
            df = df.filter(pl.col('fiscal') < 2024)
        else:
            df = df.filter(pl.col('year') < 2025)

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
                    pl.col("year").cast(pl.Utf8) + "-" + pl.col("month").cast(pl.Utf8).str.zfill(2)
                ).alias(frequency)
            )
            df = df.sort(frequency)
        elif time_frame == "quarterly":
            frequency = "year_quarter"
            df = df.with_columns(
                (
                    pl.col("year").cast(pl.String) + "-q" + pl.col("quarter").cast(pl.String)
                ).alias(frequency)
            )
            df = df.sort(frequency)

        df = df.filter(pl.col(column) != 0)
        min_idx = df.select(pl.col(column).arg_min()).item()
        max_idx = df.select(pl.col(column).arg_max()).item()

        range_min = df[column][min_idx] - df[column][min_idx]*.2
        range_max = df[column][max_idx] + df[column][max_idx]*.2

        chart_width = 'container'

        x_values = df.select(frequency).unique().to_series().to_list()

        if time_frame == "monthly":
            tick_vals = x_values[::6]
        elif time_frame == "quarterly":
            tick_vals = x_values[::3]
        else:
            tick_vals = x_values

        chart = (
            alt.Chart(df)
            .mark_line()
            .encode(
                x=alt.X(f"{frequency}:N", title="", axis=alt.Axis(values=tick_vals)),
                y=alt.Y(f"{column}:Q", title=f"", scale=alt.Scale(domain=[range_min, range_max])),
                tooltip=[
                    alt.Tooltip(f"{frequency}:N", title="Periodo"),
                    alt.Tooltip(f"{column}:Q",)
                ]
            )
            .properties(width=chart_width, padding={"top": 10, "bottom": 10, "left": 30})
        ).configure_view(
            fill='#e6f7ff'
        ).configure_axis(
            gridColor='white',
            grid=True
        )

        return chart, columns
    
    def create_consumer_graph(self, time_frame: str) -> alt.Chart:
        df = self.consumer_data(time_frame)
        df = df.fill_null(0).fill_nan(0)

        # print(df.columns)
        exclude_columns = ["date", "month", "year", "quarter", "fiscal"]

        dropdown = alt.binding_select(
            options=[col for col in df.columns if col not in exclude_columns],
            name="Y-axis column",
        )
        ycol_param = alt.param(value="ropa", bind=dropdown)

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
                    pl.col("year").cast(pl.Utf8) + "-" + pl.col("month").cast(pl.Utf8).str.zfill(2)
                ).alias(frequency)
            )
            df = df.sort(frequency)
        elif time_frame == "quarterly":
            frequency = "year_quarter"
            df = df.with_columns(
                (
                    pl.col("year").cast(pl.String) + "-q" + pl.col("quarter").cast(pl.String)
                ).alias(frequency)
            )
            df = df.sort(frequency)

        num_points = len(df[frequency].unique())

        if time_frame == "fiscal" or time_frame == "yearly":
            chart_width = 'container'
        else:
            chart_width = max(600, num_points * 15)

        chart = (
            alt.Chart(df)
            .mark_line()
            .encode(
                x=alt.X(f"{frequency}:N", title=""),
                y=alt.Y("y:Q", title=f""),
                tooltip=[
                    alt.Tooltip(f"{frequency}:N", title="Periodo"),
                    alt.Tooltip(f"y:Q",)
                ]
            )
            .transform_calculate(y=f"datum[{ycol_param.name}]")
            .add_params(ycol_param)
            .properties(width=chart_width, padding={"top": 10, "bottom": 10, "left": 30})
        ).configure_view(
            fill='#e6f7ff'
        ).configure_axis(
            gridColor='white',
            grid=True
        )

        return chart



