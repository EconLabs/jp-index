from .data_pull import DataPull
from datetime import datetime
import polars.selectors as cs
import polars as pl
import os
from ..models import init_indicators_table


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
        df = self.insert_consumer()
        variables = df.columns
        remove = ["id", "date", "month", "year", "quarter", "fiscal"]
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

    def insert_jp_index(self, update: bool = False) -> pl.DataFrame:
        """
        Processes the economic indicators data and stores it in the database.
        If the data does not exist, it will pull the data from the source.

        Parameters
        ----------
        update : bool
            Whether to update the data. Defaults to False.

        Returns
        -------
        pl.DataFrame
        """

        if (
            not os.path.exists(f"{self.saving_dir}raw/economic_indicators.xlsx")
            or update
        ):
            self.pull_economic_indicators(
                f"{self.saving_dir}raw/economic_indicators.xlsx"
            )
        if (
            "indicatorstable"
            not in self.conn.sql("SHOW TABLES;").df().get("name").tolist()
        ):
            init_indicators_table(self.data_file)
        if self.conn.sql("SELECT * FROM 'indicatorstable';").df().empty:
            jp_df = self.process_sheet(
                f"{self.saving_dir}raw/economic_indicators.xlsx", 3
            )

            for sheet in range(4, 20):
                df = self.process_sheet(
                    f"{self.saving_dir}raw/economic_indicators.xlsx", sheet
                )
                jp_df = jp_df.join(df, on=["date"], how="left", validate="1:1")

            jp_df = jp_df.sort(by="date").with_columns(
                id=pl.col("date").rank().cast(pl.Int64)
            )
            self.conn.sql("INSERT INTO 'indicatorstable' BY NAME SELECT * FROM jp_df;")
            return self.conn.sql("SELECT * FROM 'indicatorstable';").pl()
        else:
            return self.conn.sql("SELECT * FROM 'indicatorstable';").pl()

    def process_sheet(self, file_path: str, sheet_id: int) -> pl.DataFrame:
        """
        Processes a sheet from the economic indicators data and returns a DataFrame

        Parameters
        ----------
        file_path : str
            The path to the Excel file

        sheet_id : int
            The sheet ID to process

        Returns
        -------
        pl.DataFrame
        """
        df = pl.read_excel(file_path, sheet_id=sheet_id)
        months = [
            "Enero",
            "Febrero",
            "Marzo",
            "Abril",
            "Mayo",
            "Junio",
            "Julio",
            "Agosto",
            "Septiembre",
            "Octubre",
            "Noviembre",
            "Diciembre",
            "Meses",
        ]
        col_name = self.clean_name(df.columns[1])

        df = df.filter(pl.nth(1).is_in(months)).drop(cs.first()).head(13)
        columns = df.head(1).with_columns(pl.all()).cast(pl.String).to_dicts().pop()
        for item in columns:
            if columns[item] == "Meses":
                continue
            elif columns[item] is None:
                df = df.drop(item)
            elif (
                float(columns[item]) < 2000
                or float(columns[item]) > datetime.now().year + 1
            ):
                df = df.drop(item)

        if len(df.columns) > (datetime.now().year - 1997):
            df = df.select(pl.nth(range(0, len(df.columns) // 2)))

        df = df.rename(
            df.head(1)
            .with_columns(pl.nth(range(1, len(df.columns))).cast(pl.Int64))
            .cast(pl.String)
            .to_dicts()
            .pop()
        ).tail(-1)
        df = df.with_columns(pl.col("Meses").str.to_lowercase()).cast(pl.String)
        df = self.process_panel(df, col_name)

        return df

    def process_panel(self, df: pl.DataFrame, col_name: str) -> pl.DataFrame:
        """
        Processes the data and turns it into a panel DataFrame

        Parameters
        ----------
        df : pl.DataFrame
            The DataFrame to process
        col_name : str
            The name of the column to process

        Returns
        -------
        pl.DataFrame
        """
        empty_df = [
            pl.Series("date", [], dtype=pl.Datetime),
            pl.Series(col_name, [], dtype=pl.Float64),
        ]
        clean_df = pl.DataFrame(empty_df)

        for column in df.columns:
            if column == "Meses":
                continue
            column_name = col_name
            # Create a temporary DataFrame
            tmp = df
            tmp = tmp.rename({column: column_name})
            tmp = tmp.with_columns(
                Meses=pl.col("Meses").str.strip_chars().str.to_lowercase()
            )
            tmp = tmp.with_columns(
                pl.when(pl.col("Meses") == "enero")
                .then(1)
                .when(pl.col("Meses") == "febrero")
                .then(2)
                .when(pl.col("Meses") == "marzo")
                .then(3)
                .when(pl.col("Meses") == "abril")
                .then(4)
                .when(pl.col("Meses") == "mayo")
                .then(5)
                .when(pl.col("Meses") == "junio")
                .then(6)
                .when(pl.col("Meses") == "julio")
                .then(7)
                .when(pl.col("Meses") == "agosto")
                .then(8)
                .when(pl.col("Meses") == "septiembre")
                .then(9)
                .when(pl.col("Meses") == "octubre")
                .then(10)
                .when(pl.col("Meses") == "noviembre")
                .then(11)
                .when(pl.col("Meses") == "diciembre")
                .then(12)
                .alias("month")
            )
            tmp = tmp.with_columns(
                (
                    pl.col(column_name)
                    .str.replace_all("$", "", literal=True)
                    .str.replace_all("(", "", literal=True)
                    .str.replace_all(")", "", literal=True)
                    .str.replace_all(",", "")
                    .str.replace_all("-", "")
                    .str.strip_chars()
                    .alias(column_name)
                )
            )
            tmp = tmp.with_columns(
                pl.when(pl.col(column_name) == "n/d")
                .then(None)
                .when(pl.col(column_name) == "**")
                .then(None)
                .when(pl.col(column_name) == "-")
                .then(None)
                .when(pl.col(column_name) == "no disponible")
                .then(None)
                .otherwise(pl.col(column_name))
                .alias(column_name)
            )
            tmp = tmp.select(
                pl.col("month").cast(pl.Int64).alias("month"),
                pl.lit(int(column)).cast(pl.Int64).alias("year"),
                pl.col(column_name).cast(pl.Float64).alias(column_name),
            )

            tmp = tmp.with_columns(
                (
                    pl.col("year").cast(pl.String)
                    + "-"
                    + pl.col("month").cast(pl.String)
                    + "-01"
                ).alias("date")
            )
            tmp = tmp.select(
                pl.col("date").str.to_datetime("%Y-%m-%d").alias("date"),
                pl.col(column_name).alias(column_name),
            )

            clean_df = pl.concat([clean_df, tmp], how="vertical")
        return clean_df
