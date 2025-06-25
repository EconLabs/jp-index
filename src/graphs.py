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
                    alt.Tooltip("federal_action_obligation:Q", title="federal action obligation"),
                    alt.Tooltip(f"{category}:N", title=category.replace('_', ' ')),
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

        df = df.sort(period)
        x_values = df.select(period).unique().to_series().to_list()

        if type == "monthly":
            tick_vals = x_values[::6]
        elif type == "quarterly":
            tick_vals = x_values[::3]
        else:
            tick_vals = x_values

        chart_width = "container"

        data_chart = (
            alt.Chart(grouped_pd)
            .mark_line()
            .encode(
                x=alt.X(f"{period}:O", title="", sort=sort_expr, axis=alt.Axis(values=tick_vals)),
                y=alt.Y(
                    "federal_action_obligation:Q",
                    title="",
                ),
                tooltip=[
                    alt.Tooltip(f"{period}:O", title="period"),
                    alt.Tooltip(
                        f"federal_action_obligation:Q",
                        title="federal action obligation",
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

        mapping_dict = {
            "indice_de_actividad_economica": "Indice de Actividad Economica del Banco de Desarrollo Economico",
            "encuesta_de_grupo_trabajador_ajustada_estacionalmente": "Grupo Trabajador (Miles de Personas) AJUSTADO ESTACIONALMENTE",
            "encuesta_de_grupo_trabajador": "Poblacion Civil No-Institucional (Miles de Personas)",
            "encuesta_de_establecimientos_ajustados_estacionalmente": "Empleo No Agricola: Todas las Industrias (Miles de Personas) AJUSTADOS ESTACIONALMENTE",
            "encuesta_de_establecimientos": "Empleo No Agricola: Todas las Industrias (Miles de Personas)",
            "indicadores_de_turismo": "Total de Registros en Hoteles y Paradores",
            "indicadores_de_construccion": "Numero de Unidades de Vivienda Vendidas en Puerto Rico",
            "indicadores_de_ingresos_netos": "Ingreso Neto al Fondo General - Miles de Dolares",
            "indicadores_de_energia_electrica": "Generacion de Energia Electrica (Millones de Kilovatios / Hora)",
            "indicadores_de_quiebras": "Numero Total de Quiebras en Puerto Rico",
            "indicadores_de_comercio_exterior": "Total de Exportaciones - Miles de Dolares",
            "indicadores_de_ventas_al_detalle_a_precios_corrientes": "Total de Ventas al Detalle - A precios corrientes",
            "indicadores_de_transportacion": "Movimiento de pasajeros en el aeropuerto Luis Muñoz Marin (SJU)"
        }

        exclude_columns = ["date", "month", "year", "quarter", "fiscal"]
        columns = [
            {
                "value": col,
                "label": mapping_dict.get(col, col.replace("_", " ").capitalize())
            }
            for col in df.columns
            if col not in exclude_columns
        ]
        columns = sorted(columns, key=lambda x: x["label"])

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
            if col not in exclude_columns and not col.endswith("_lag")
        ]
        if time_frame == 'fiscal':
            df = df.filter(pl.col("fiscal") != 2025)
            df = df.filter(pl.col("fiscal") != 0)
        else:
            df = df.filter(pl.col("year") != 2025)
            df = df.filter(pl.col("year") !=0)

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
                        f"{frequency}:N", title="", axis=alt.Axis(values=tick_vals), scale=alt.Scale(zero=False)
                    ),
                    y=alt.Y(f"{column}:Q", title=f"", scale=alt.Scale(zero=False)),
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
    
    def create_jp_cycles_graphs(self, column: str):
        df = pl.from_pandas(self.jp_cycle_data())

        selected_colums = [
            col for col in df.columns
            if col != "date" and col != "year" and col != "quarter"
        ]
        
        df = df.select(["date"] + selected_colums)

        mapping_dict = {
            "indice_de_actividad_economica": "Indice de Actividad Economica del Banco de Desarrollo Economico",
            "encuesta_de_grupo_trabajador_ajustada_estacionalmente": "Grupo Trabajador (Miles de Personas) AJUSTADO ESTACIONALMENTE",
            "encuesta_de_grupo_trabajador": "Poblacion Civil No-Institucional (Miles de Personas)",
            "encuesta_de_establecimientos_ajustados_estacionalmente": "Empleo No Agricola: Todas las Industrias (Miles de Personas) AJUSTADOS ESTACIONALMENTE",
            "encuesta_de_establecimientos": "Empleo No Agricola: Todas las Industrias (Miles de Personas)",
            "indicadores_de_turismo": "Total de Registros en Hoteles y Paradores",
            "indicadores_de_construccion": "Numero de Unidades de Vivienda Vendidas en Puerto Rico",
            "indicadores_de_ingresos_netos": "Ingreso Neto al Fondo General - Miles de Dolares",
            "indicadores_de_energia_electrica": "Generacion de Energia Electrica (Millones de Kilovatios / Hora)",
            "indicadores_de_quiebras": "Numero Total de Quiebras en Puerto Rico",
            "indicadores_de_comercio_exterior": "Total de Exportaciones - Miles de Dolares",
            "indicadores_de_ventas_al_detalle_a_precios_corrientes": "Total de Ventas al Detalle - A precios corrientes",
            "indicadores_de_transportacion": "Movimiento de pasajeros en el aeropuerto Luis Muñoz Marin (SJU)"
        }

        columns_dict = [
            {"value": col, "label": mapping_dict.get(col, col.replace("_", " ").capitalize())}
            for col in selected_colums
            if 'cycle' not in col and 'trend' not in col
        ]
        columns_dict = sorted(columns_dict, key=lambda x: x["label"])

        df = df[["date", column, f"{column}_cycle", f"{column}_trend"]].rename({
            column: "Original",
            f"{column}_cycle": "Cycle",
            f"{column}_trend": "Trend"
        })
        df = df.melt(
            id_vars="date",
            value_vars=["Original", "Cycle", "Trend"]
        )

        chart_width = "container"
        
        chart = (
            (
                alt.Chart(df)
                .mark_line()
                .encode(
                    x=alt.X("date:N", title=""),
                    y=alt.Y("value:Q", title=""),
                    color=alt.Color("variable:N", title=""),
                    tooltip=[
                        alt.Tooltip("date:N", title="Periodo"),
                        alt.Tooltip("variable:N", title="Serie"),
                        alt.Tooltip("value:Q", title="Valor")
                    ]
                )
                .properties(
                    width=chart_width, padding={"top": 10, "bottom": 10, "left": 30}
                )
            )
            .configure_view(fill="#e6f7ff")
            .configure_axis(gridColor="white", grid=True)
        )

        return chart, columns_dict

    def create_spending_chart(self, period: str, metric: str):
        df_grouped, columns = self.process_spending_data(period, metric)

        month_map = {
            "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4,
            "May": 5, "Jun": 6, "Jul": 7, "Aug": 8,
            "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12
        }

        months_lookup = pl.DataFrame({
            "month_name": list(month_map.keys()),
            "month_int":   list(month_map.values())
        })

        df_aux = df_grouped.with_columns([
            pl.col("time_period").str.extract(r"^(\d+)", 1).cast(pl.Int32).alias("year_int"),

            pl.when(pl.col("time_period").str.contains(r"-q[1-4]$"))
            .then(pl.col("time_period").str.extract(r"-q([1-4])$", 1).cast(pl.Int32))
            .when(pl.col("time_period").str.contains(r"-(\d+)$"))
            .then(pl.col("time_period").str.extract(r"-(\d+)$", 1).cast(pl.Int32))
            .otherwise(pl.lit(None))
            .alias("period_int_raw"),

            pl.when(pl.col("time_period").str.contains(r"-[A-Za-z]+$"))
            .then(pl.col("time_period").str.extract(r"-(\w+)$", 1))
            .otherwise(pl.lit(None))
            .alias("month_name_raw")
        ])

        df_joined = df_aux.join(
            months_lookup,
            left_on="month_name_raw",
            right_on="month_name",
            how="left"
        )

        df_sorted = (
            df_joined
            .with_columns([
                pl.coalesce([
                    pl.col("period_int_raw"),
                    pl.col("month_int"),
                    pl.lit(1)
                ]).alias("period_int")
            ])
            .sort(["year_int", "period_int"])
        )

        # 🔐 Drop seguro
        columns_to_drop = [
            "year_int",
            "period_int_raw",
            "month_name_raw",
            "month_name",
            "month_int",
            "period_int"
        ]
        df_sorted = df_sorted.drop([col for col in columns_to_drop if col in df_sorted.columns])

        x_order = df_sorted["time_period"].to_list()

        if period.lower() == "monthly":
            tick_vals = x_order[::6]
        elif period.lower() == "quarterly":
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
                    axis=alt.Axis(labelAngle=-45, values=tick_vals)
                ),
                y=alt.Y(
                    f"{metric}:Q",
                    title="USD",
                    axis=alt.Axis(format=".0f")
                ),
                tooltip=[
                    alt.Tooltip("time_period:N", title="Periodo"),
                    alt.Tooltip(f"{metric}:Q", title="USD", format=".0f")
                ]
            )
            .properties(
                width="container",
                height=300,
                title=f"Evolución de {metric.replace('_',' ')} ({period.capitalize()})"
            )
            .configure_view(fill="#e6f7ff")
            .configure_axis(grid=True, gridColor="white")
            .interactive(bind_y=False)
        )

        return chart, columns

    def create_demographic_graph(self, time_frame: str, column: str):
        df = self.jp_demographic_data(time_frame)

        excluded_columns = ['time_period', 'year', 'fiscal_year', 'month', 'quarter']

        columns = [
            {"value": "componentes", "label": "Componentes"}
        ] + [
            {"value": col, "label": col.replace("_", " ").capitalize()}
            for col in df.columns
            if col not in excluded_columns
        ]
        
        chart_width = "container"

        x_values = df.select('time_period').unique().to_series().to_list()

        if time_frame == "monthly":
            tick_vals = x_values[::6]
        elif time_frame == "quarterly":
            tick_vals = x_values[::3]
        else:
            tick_vals = x_values
        
        if column == 'componentes':
            df = df.drop("populacion")
            df = df.melt(
                id_vars="time_period",
                value_vars=["nacimientos", "muertes", "migraciones"]
            )
            
            chart = (
                (
                    alt.Chart(df)
                    .mark_line()
                    .encode(
                        x=alt.X("time_period:N", title="", axis=alt.Axis(values=tick_vals)),
                        y=alt.Y("value:Q", title=""),
                        color=alt.Color("variable:N", title=""),
                        tooltip=[
                            alt.Tooltip("time_period:N", title="Periodo"),
                            alt.Tooltip("variable:N", title="Serie"),
                            alt.Tooltip("value:Q", title="Valor")
                        ]
                    )
                    .properties(
                        width=chart_width, padding={"top": 10, "bottom": 10, "left": 30}
                    )
                )
                .configure_view(fill="#e6f7ff")
                .configure_axis(gridColor="white", grid=True)
            )
        else:
            chart = (
                (
                    alt.Chart(df)
                    .mark_line()
                    .encode(
                        x=alt.X(
                            f"time_period:N", title="", scale=alt.Scale(zero=False), axis=alt.Axis(values=tick_vals)
                        ),
                        y=alt.Y(f"{column}:Q", title=f"", scale=alt.Scale(zero=False)),
                        tooltip=[
                            alt.Tooltip(f"time_period:N", title="Periodo"),
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
