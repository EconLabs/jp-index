import logging
from ..models import init_consumer_table, init_indicators_table, init_activity_table
from .data_pull import DataPull
from datetime import datetime
from ibis import _
import ibis.expr.types as it
import polars.selectors as cs
import polars as pl
import ibis
import os
from .models import init_award_data_table
import logging



class DataIndex(DataPull):
    """
    Data processing class that calculates multiple indicators from the DataPull class
    """
    def __init__(
        self,
        database_url: str = "sqlite:///db.sqlite",
        debug: bool = False,
    ):
        """
        Constructor for the DataProcess class. Creates a connection to the database and
        creates the data directory if it does not exist.

    def __init__(
        self,
        saving_dir: str = "data/",
        database_file: str = "data.ddb",
        log_file: str = "data_process.log",
    ):
        super().__init__(saving_dir, database_file, log_file)

        Returns
        -------
        DataProcess
        """
        data_dir = 'data/'
        self.debug = debug
        super().__init__(debug)
        self.database_url = database_url
        self.engine = create_engine(self.database_url)
        self.data_dir = data_dir

        if self.database_url.startswith("sqlite"):
            self.conn = ibis.sqlite.connect(self.database_url.replace("sqlite:///", ""))
        elif self.database_url.startswith("postgres"):
            self.conn = ibis.postgres.connect(
                user=self.database_url.split("://")[1].split(":")[0],
                password=self.database_url.split("://")[1].split(":")[1].split("@")[0],
                host=self.database_url.split("://")[1].split(":")[1].split("@")[1],
                port=self.database_url.split("://")[1].split(":")[2].split("/")[0],
                database=self.database_url.split("://")[1].split(":")[2].split("/")[1],
            )
        else:
            raise Exception("Database url is not supported")

        if not os.path.exists(f"{data_dir}/raw"):
            os.makedirs(f"{data_dir}/raw")
        if not os.path.exists(f"{data_dir}/processed"):
            os.makedirs(f"{data_dir}/processed")
        if self.database_url.startswith("sqlite"):
            self.conn = ibis.sqlite.connect(self.database_url.replace("sqlite:///", ""))
        elif self.database_url.startswith("postgres"):
            self.conn = ibis.postgres.connect(
                user=self.database_url.split("://")[1].split(":")[0],
                password=self.database_url.split("://")[1].split(":")[1].split("@")[0],
                host=self.database_url.split("://")[1].split(":")[1].split("@")[1],
                port=self.database_url.split("://")[1].split(":")[2].split("/")[0],
                database=self.database_url.split("://")[1].split(":")[2].split("/")[1],
            )

    def process_awards(self, fiscal_year: int, update: bool = False) -> it.Table:
        if (
            "AwardTable" not in self.conn.list_tables()
            or self.conn.table("AwardTable").count().execute() == 0
            or update
        ): 
            data_file = self.database_url.split("///")[1]
            init_award_data_table(data_file)

        try:
            result = self.conn.sql(f"SELECT COUNT(*) FROM AwardTable WHERE fiscal_year = {fiscal_year}").execute()
            if not result.iloc[0, 0]:
                df = DataPull.pull_awards_by_year(self, fiscal_year)
                if df.is_empty():
                    return self.conn.table("AwardTable")
                self.conn.insert("AwardTable", df)    
                logging.info(f"Inserted fiscal year {fiscal_year} to sqlite table.")
            else:
                logging.info(f"Fiscal year {fiscal_year} already in db.")
        except Exception as e:
            logging.error(f"Error inserting fiscal year {fiscal_year} to sqlite table. {e}")
            return self.conn.table("AwardTable")

    def process_consumer(self, update: bool = False) -> ibis.expr.types.relations.Table:
        """
        Processes the consumer data and stores it in the database. If the data does
        not exist, it will pull the data from the source.

        Parameters
        ----------
        update : bool
            Whether to update the data. Defaults to False.

        Returns
        -------
        pl.DataFrame
        """
        if not os.path.exists(f"{self.data_dir}/raw/consumer.xls") or update:
            self.pull_consumer(f"{self.data_dir}/raw/consumer.xls")
        if (
            "consumertable" not in self.conn.list_tables()
            or self.conn.table("consumertable").count().execute() == 0
            or update
        ):
            create_consumer_table(self.engine)
            df = pl.read_excel(f"{self.data_dir}/raw/consumer.xls", sheet_id=1)
            names = df.head(1).to_dicts().pop()
            names = {k: self.clean_name(v) for k, v in names.items()}
            df = df.rename(names)
            df = df.tail(-2).head(-1)
            df = df.with_columns(pl.col("descripcion").str.to_lowercase())
            df = df.with_columns(
                (
                    pl.when(pl.col("descripcion").str.contains("ene"))
                    .then(
                        pl.col("descripcion")
                        .str.replace("ene", "01")
                        .str.split_exact("-", 1)
                        .struct.rename_fields(["month", "year"])
                        .alias("date")
                    )
                    .when(pl.col("descripcion").str.contains("feb"))
                    .then(
                        pl.col("descripcion")
                        .str.replace("feb", "02")
                        .str.split_exact("-", 1)
                        .struct.rename_fields(["month", "year"])
                        .alias("date")
                    )
                    .when(pl.col("descripcion").str.contains("mar"))
                    .then(
                        pl.col("descripcion")
                        .str.replace("mar", "03")
                        .str.split_exact("-", 1)
                        .struct.rename_fields(["month", "year"])
                        .alias("date")
                    )
                    .when(pl.col("descripcion").str.contains("abr"))
                    .then(
                        pl.col("descripcion")
                        .str.replace("abr", "04")
                        .str.split_exact("-", 1)
                        .struct.rename_fields(["month", "year"])
                        .alias("date")
                    )
                    .when(pl.col("descripcion").str.contains("may"))
                    .then(
                        pl.col("descripcion")
                        .str.replace("may", "05")
                        .str.split_exact("-", 1)
                        .struct.rename_fields(["month", "year"])
                        .alias("date")
                    )
                    .when(pl.col("descripcion").str.contains("jun"))
                    .then(
                        pl.col("descripcion")
                        .str.replace("jun", "06")
                        .str.split_exact("-", 1)
                        .struct.rename_fields(["month", "year"])
                        .alias("date")
                    )
                    .when(pl.col("descripcion").str.contains("jul"))
                    .then(
                        pl.col("descripcion")
                        .str.replace("jul", "07")
                        .str.split_exact("-", 1)
                        .struct.rename_fields(["month", "year"])
                        .alias("date")
                    )
                    .when(pl.col("descripcion").str.contains("ago"))
                    .then(
                        pl.col("descripcion")
                        .str.replace("ago", "08")
                        .str.split_exact("-", 1)
                        .struct.rename_fields(["month", "year"])
                        .alias("date")
                    )
                    .when(pl.col("descripcion").str.contains("sep"))
                    .then(
                        pl.col("descripcion")
                        .str.replace("sep", "09")
                        .str.split_exact("-", 1)
                        .struct.rename_fields(["month", "year"])
                        .alias("date")
                    )
                    .when(pl.col("descripcion").str.contains("oct"))
                    .then(
                        pl.col("descripcion")
                        .str.replace("oct", "10")
                        .str.split_exact("-", 1)
                        .struct.rename_fields(["month", "year"])
                        .alias("date")
                    )
                    .when(pl.col("descripcion").str.contains("nov"))
                    .then(
                        pl.col("descripcion")
                        .str.replace("nov", "11")
                        .str.split_exact("-", 1)
                        .struct.rename_fields(["month", "year"])
                        .alias("date")
                    )
                    .when(pl.col("descripcion").str.contains("dic"))
                    .then(
                        pl.col("descripcion")
                        .str.replace("dic", "12")
                        .str.split_exact("-", 1)
                        .struct.rename_fields(["month", "year"])
                        .alias("date")
                    )
                    .otherwise(
                        pl.col("descripcion")
                        .str.split_exact("-", 1)
                        .struct.rename_fields(["year", "month"])
                        .alias("date")
                    )
                )
            ).unnest("date")
            df = df.with_columns(year=pl.col("year").str.strip_chars())
            df = df.with_columns(
                (
                    pl.when(
                        (pl.col("year").str.len_chars() == 2)
                        & (pl.col("year").str.strip_chars().cast(pl.Int32) < 80)
                    )
                    .then(pl.col("year").str.strip_chars().cast(pl.Int32) + 2000)
                    .when(
                        (pl.col("year").str.len_chars() == 2)
                        & (pl.col("year").str.strip_chars().cast(pl.Int32) >= 80)
                    )
                    .then(pl.col("year").str.strip_chars().cast(pl.Int32) + 1900)
                    .otherwise(pl.col("year").str.strip_chars().cast(pl.Int32))
                    .alias("year")
                )
            )
            df = df.with_columns(
                date=pl.date(pl.col("year").cast(pl.String), pl.col("month"), 1)
            ).sort(by="date")
            df = df.with_columns(pl.col("date").cast(pl.String))
            df = df.drop(["year", "month", "descripcion"])
            df = df.with_columns(pl.all().exclude("date").cast(pl.Float64))
            df = df.with_columns(
                year=pl.col("date").str.slice(0, 4).cast(pl.Int64),
                month=pl.col("date").str.slice(5, 2).cast(pl.Int64),
            )
            df = df.with_columns(
                pl.when((pl.col("month") >= 1) & (pl.col("month") <= 3))
                .then(1)
                .when((pl.col("month") >= 4) & (pl.col("month") <= 6))
                .then(2)
                .when((pl.col("month") >= 7) & (pl.col("month") <= 9))
                .then(3)
                .when((pl.col("month") >= 10) & (pl.col("month") <= 12))
                .then(4)
                .otherwise(0)
                .alias("quarter"),
                pl.when(pl.col("month") > 6)
                .then(pl.col("year") + 1)
                .otherwise(pl.col("year"))
                .alias("fiscal"),
            )
            self.conn.insert("consumertable", df)
            logging.info("Inserted data into consumertable")
            return self.conn.table("consumertable")
        else:
            return self.conn.table("consumertable")

    def consumer_data(self, agg: str) -> it.Table:
        df = self.process_consumer()
        variables = df.columns
        remove = ["id", "date", "month", "year", "quarter", "fiscal"]
        variables = [var for var in variables if var not in remove]
        aggregation_exprs = {var: getattr(_, var).sum().name(var) for var in variables}

        match agg:
            case "monthly":
                return df
            case "quarterly":
                return df.group_by(["year", "quarter"]).aggregate(**aggregation_exprs)
            case "yearly":
                return df.group_by("year").aggregate(**aggregation_exprs)
            case "fiscal":
                return df.group_by("fiscal").aggregate(**aggregation_exprs)
            case _:
                raise ValueError("Invalid aggregation")

    def clean_name(self, name: str) -> str:
        """
        Cleans the name of a column by converting it to lowercase, removing special characters,
        and replacing accented characters with their non-accented counterparts.

        Parameters
        ----------
        name : str

        Returns
        -------
        str

        """
        cleaned = name.lower().strip()
        cleaned = cleaned.replace("-", " ").replace("=", "")
        cleaned = cleaned.replace("  ", "_").replace(" ", "_")
        cleaned = cleaned.replace("*", "").replace(",", "")
        cleaned = cleaned.replace("__", "_")
        cleaned = cleaned.replace(")", "").replace("(", "")
        replacements = {"á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u", "ñ": "n"}
        for old, new in replacements.items():
            cleaned = cleaned.replace(old, new)
        return cleaned

    def process_jp_index(self, update: bool = False) -> ibis.expr.types.relations.Table:
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
            not os.path.exists(f"{self.data_dir}/raw/economic_indicators.xlsx")
            or update
        ):
            self.pull_economic_indicators(
                f"{self.data_dir}/raw/economic_indicators.xlsx"
            )
        if (
            "indicatorstable" not in self.conn.list_tables()
            or self.conn.table("indicatorstable").count().execute() == 0
            or update
        ):
            create_indicators_table(self.engine)

            jp_df = self.process_sheet(
                f"{self.data_dir}/raw/economic_indicators.xlsx", 3
            )

            for sheet in range(4, 20):
                df = self.process_sheet(
                    f"{self.data_dir}/raw/economic_indicators.xlsx", sheet
                )
                jp_df = jp_df.join(df, on=["date"], how="left", validate="1:1")

            jp_df = jp_df.sort(by="date").with_columns(
                id=pl.col("date").rank().cast(pl.Int64)
            )
            self.conn.insert("indicatorstable", jp_df)
            return self.conn.table("indicatorstable")
        else:
            return self.conn.table("indicatorstable")

    def jp_index_data(self, agg: str) -> it.Table:
        df = self.process_jp_index()
        variables = df.columns
        remove = ["id", "date"]

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

    def process_activity(self, update: bool = False) -> ibis.expr.types.relations.Table:
        if not os.path.exists(f"{self.data_dir}/raw/activity.xls") or update:
            self.pull_consumer(f"{self.data_dir}/raw/activity.xls")
        if (
            "activitytable" not in self.conn.list_tables()
            or self.conn.table("activitytable").count().execute() == 0
            or update
        ):

            df = pl.read_excel(f"{self.data_dir}/raw/activity.xls", sheet_id=3)
            df = df.select(pl.nth(0), pl.nth(1))
            df = df.filter(
                (pl.nth(0).str.strip_chars().str.len_chars() <= 8)
                & (pl.nth(0).str.strip_chars().str.len_chars() >= 6)
            )
            df = df.with_columns(pl.nth(0).str.to_lowercase())
            df = df.with_columns(date=pl.nth(0).str.replace("m", "-") + "-01")
            df = df.select(
                date=pl.col("date").str.to_datetime(), index=pl.nth(1).cast(pl.Float64)
            )
            self.conn.insert("activitytable", df)
