import polars as pl
import pandas as pd
import statsmodels.api as sm
from statsmodels.tsa.vector_ar.var_model import var_acf

from .data_pull import DataPull


class DataIndex(DataPull):
    """
    Data processing class that calculates multiple indicators from the DataPull class
    """

    def __init__(
        self,
        saving_dir: str = "data/",
        database_file: str = "data.ddb",
        log_file: str = "data_process.log",
    ):
        """
        Initialize the DataIndex class.

        Parameters
        ----------
        saving_dir: str
            Directory to save the data.
        database_file: str
            file path that will save the duckdb instance
        log_file: str
            file path that will save the log messegases

        Returns
        -------
        None
        """
        super().__init__(saving_dir, database_file, log_file)

    def consumer_data(self, time_frame: str) -> pl.DataFrame:
        """
        Aggregate consumer data based on the specified time frame.

        Parameters
        ----------
        time_frame: str
            The time frame for aggregation. Valid options are:
            - "monthly": No aggregation, returns the raw data.
            - "quarterly": Aggregates data by year and quarter.
            - "yearly": Aggregates data by year.
            - "fiscal": Aggregates data by fiscal period.

        Returns
        -------
        pl.DataFrame
            A Polars DataFrame with aggregated data based on the specified time frame.

        Raises
        ------
        ValueError
            If an invalid time frame is provided.
        """
        df = self.insert_consumer()
        variables = df.columns
        remove = ["date", "month", "year", "quarter", "fiscal"]
        variables = [var for var in variables if var not in remove]
        aggregation_exprs = [pl.col(var).sum().alias(var) for var in variables]

        match time_frame:
            case "monthly":
                return df
            case "quarterly":
                return df.group_by(["year", "quarter"]).agg(aggregation_exprs)
            case "yearly":
                return df.group_by("year").agg(aggregation_exprs)
            case "fiscal":
                return df.group_by("fiscal").agg(aggregation_exprs)
            case _:
                raise ValueError("Invalid aggregation")

    def apply_data_type(self, df: pl.DataFrame, data_type: str):
        value_columns = [
            col
            for col in df.columns
            if col not in {"year", "month", "fiscal", "date", "quarter"}
        ]

        lag_df = (
            df.select(["year", "month"] + value_columns)
            .with_columns([(pl.col("year") + 1).alias("year")])
            .rename({col: f"{col}_lag" for col in value_columns})
        )

        df = df.join(lag_df, on=["year", "month"], how="left")

        for col in value_columns:
            if data_type == "cambio_porcentual":
                transformation = (
                    ((pl.col(col) - pl.col(f"{col}_lag")).cast(pl.Float64))
                    / (pl.col(f"{col}_lag").cast(pl.Float64))
                    * 100
                ).alias(col)
            else:
                transformation = (pl.col(col) - pl.col(f"{col}_lag")).alias(col)

            df = df.with_columns(transformation)
        df = df.select(df.columns)

        df = df.with_columns(
            [
                pl.col(col).dt.total_microseconds().alias(col)
                if df.schema[col] == pl.Duration("us")
                else pl.col(col)
                for col in df.columns
            ]
        )
        return df

    def process_consumer_data(self, time_frame: str, data_type: str) -> pl.DataFrame:
        if data_type == "cambio_porcentual":
            df = self.consumer_data("monthly")
            df = self.apply_data_type(df, data_type)
            df = df.filter(pl.col("year") != 1984)
        elif data_type == "primera_diferencia":
            df = self.consumer_data("monthly")
            df = self.apply_data_type(df, data_type)
            df = df.filter(pl.col("year") != 1984)
        elif data_type == "indices_precio":
            df = self.consumer_data(time_frame)
            df = df

        return df

    def jp_indicator_data(self, time_frame: str) -> pl.DataFrame:
        """
        Aggregate consumer data based on the specified time frame.

        Parameters
        ----------
        time_frame: str
            The time frame for aggregation. Valid options are:
            - "monthly": No aggregation, returns the raw data.
            - "quarterly": Aggregates data by year and quarter.
            - "yearly": Aggregates data by year.
            - "fiscal": Aggregates data by fiscal period.

        Returns
        -------
        pl.DataFrame
            A Polars DataFrame with aggregated consumer data based on the specified time frame.

        Raises
        ------
        ValueError
            If an invalid time frame is provided.
        """

        df = self.insert_jp_index()
        variables = df.columns
        remove = ["date", "month", "year", "quarter", "fiscal"]
        sum = [
            "encuesta_de_establecimientos",
            "indicadores_de_turismo",
            "indicadores_de_construccion",
            "indicadores_de_ingresos_netos",
            "indicadores_de_energia_electrica",
        ]

        variables = [var for var in variables if var not in remove and var not in sum]

        match time_frame:
            case "monthly":
                return df.sort(["year", "month"])
            case "quarterly":
                return (
                    df.group_by(["year", "quarter"])
                    .agg(
                        pl.col(variables).mean().name.suffix("_mean"),
                        pl.col(sum).sum().name.suffix("_sum"),
                    )
                    .sort(["year", "quarter"])
                )
            case "yearly":
                return (
                    df.group_by("year")
                    .agg(pl.col(variables).mean(), pl.col(sum).sum())
                    .sort(["year"])
                )
            case "fiscal":
                return (
                    df.group_by("fiscal")
                    .agg(
                        pl.col(variables).mean().name.suffix("_mean"),
                        pl.col(sum).sum().name.suffix("_sum"),
                    )
                    .sort(["fiscal"])
                )
            case _:
                raise ValueError("Invalid aggregation")

    def jp_cycle_data(self) -> pd.DataFrame:
        df = self.jp_indicator_data(time_frame="quarterly").filter(
            pl.col("year") < 2025
        )
        data = (
            df.with_columns(
                date=pl.col("year").cast(pl.String)
                + "Q"
                + pl.col("quarter").cast(pl.String)
            )
            .sort("date")
            .to_pandas()
        )
        data.set_index(data["date"], inplace=True)
        for col in data.columns:
            if col in ["year", "quarter", "date"]:
                continue
            cycle, trend = sm.tsa.filters.hpfilter(data[col], 1600)
            data[f"{col}_cycle"] = cycle
            data[f"{col}_trend"] = trend
        return data
