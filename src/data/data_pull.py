import logging
import os
import zipfile
import time
from datetime import datetime

import polars as pl
import polars.selectors as cs
import requests
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry

import pandas as pd
import re
from ..models import (
    get_conn,
    init_activity_table,
    init_awards_table,
    init_consumer_table,
    init_indicators_table,
    init_energy_table,
)


class DataPull:
    """
    Initialize the DataPull class, setting up directory paths, database connection,
    and logging configuration.

    Parameters
    ----------
    saving_dir: str, optional, default="data/"
        The directory where data will be saved. It creates subdirectories for raw,
        processed, and external data.

    database_file: str, optional, default="data.ddb"
        The file path for the DuckDB database instance.

    log_file: str, optional, default="data_process.log"
        The file path where log messages will be saved.

    Returns
    -------
    None
        Initializes the object without returning anything.

    Side Effects
    ------------
    - Creates subdirectories for "raw", "processed", and "external" within the specified
      saving directory if they do not already exist.
    - Sets up a logging configuration that writes logs to the specified log file.
    - Establishes a connection to the DuckDB database file.
    """

    def __init__(
        self,
        saving_dir: str = "data/",
        database_file: str = "data.ddb",
        log_file: str = "data_process.log",
    ):
        self.saving_dir = saving_dir
        self.data_file = database_file
        self.conn = get_conn(self.data_file)

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%d-%b-%y %H:%M:%S",
            filename=log_file,
        )
        # Check if the saving directory exists
        if not os.path.exists(self.saving_dir + "raw"):
            os.makedirs(self.saving_dir + "raw")
        if not os.path.exists(self.saving_dir + "processed"):
            os.makedirs(self.saving_dir + "processed")
        if not os.path.exists(self.saving_dir + "external"):
            os.makedirs(self.saving_dir + "external")

    def insert_consumer(self, update: bool = False) -> pl.DataFrame:
        """
        Insert or update consumer data from an Excel file into the consumer table in the database.

        Parameters
        ----------
        update: bool, optional, default=False
            Whether to force an update by pulling new data, even if the file already exists.
            If set to True, the consumer data will be pulled again regardless of the current file.

        Returns
        -------
        pl.DataFrame
            A Polars DataFrame containing the consumer data after insertion or update.

        Side Effects
        ------------
        - Pulls consumer data from an Excel file (`consumer.xls`) located in the 'raw' subdirectory if
          the file doesn't exist or if `update` is set to True.
        - Renames columns and transforms the data, including parsing and formatting the 'descripcion' field
          into a date and splitting it into year and month.
        - Adds additional columns for 'quarter' and 'fiscal' periods based on the date.
        - Inserts new data into the consumer table or returns the existing data from the table.

        Raises
        ------
        FileNotFoundError
            If the consumer Excel file is missing and `update` is set to False.
        """

        if not os.path.exists(f"{self.saving_dir}raw/consumer.xls") or update:
            self.pull_consumer(f"{self.saving_dir}raw/consumer.xls")
        if (
            "consumertable"
            not in self.conn.sql("SHOW TABLES;").df().get("name").tolist()
        ):
            init_consumer_table(self.data_file)
        if self.conn.sql("SELECT * FROM 'consumertable';").df().empty:
            df = pl.read_excel(f"{self.saving_dir}raw/consumer.xls", sheet_id=1)
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
            self.conn.sql("INSERT INTO 'consumertable' BY NAME SELECT * FROM df;")
            logging.info("Inserted data into consumertable")
            return self.conn.sql("SELECT * FROM 'consumertable';").pl()
        else:
            return self.conn.sql("SELECT * FROM 'consumertable';").pl()

    def insert_activity(self, update: bool = False) -> pl.DataFrame:
        """
        Insert or update the activity data from an Excel file into the activity table in the database.

        Parameters
        ----------
        update: bool, optional, default=False
            Whether to force an update by pulling new data, even if the file already exists.
            If set to True, the activity data will be pulled again regardless of the current file.

        Returns
        -------
        pl.DataFrame
            A Polars DataFrame containing the activity data after insertion or update.

        Side Effects
        ------------
        - Pulls activity data from an Excel file (`activity.xls`) located in the 'raw' subdirectory if
          the file doesn't exist or if `update` is set to True.
        - Initializes the activity table in the database if it does not already exist.
        - Inserts new data into the activity table or returns the existing data from the table.

        Raises
        ------
        FileNotFoundError
            If the activity Excel file is missing and `update` is set to False.
        """

        if not os.path.exists(f"{self.saving_dir}raw/activity.xls") or update:
            self.pull_consumer(f"{self.saving_dir}raw/activity.xls")
        if (
            "activitytable"
            not in self.conn.sql("SHOW TABLES;").df().get("name").tolist()
        ):
            init_activity_table(self.data_file)

        if self.conn.sql("SELECT * FROM 'consumertable';").df().empty:
            df = pl.read_excel(f"{self.saving_dir}raw/activity.xls", sheet_id=3)
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
            self.conn.sql("INSERT INTO 'activitytable' BY NAME SELECT * FROM df;")

            return self.conn.sql("SELECT * FROM 'activitytable';").pl()
        else:
            return self.conn.sql("SELECT * FROM 'activitytable';").pl()

    def clean_name(self, name: str) -> str:
        """
        Cleans and standardizes a string by converting it to lowercase, removing unwanted characters,
        and replacing accented characters with their non-accented equivalents.

        Parameters
        ----------
        name: str
            The input string that needs to be cleaned and standardized.

        Returns
        -------
        str
            The cleaned and standardized string with the following transformations:
            - Converted to lowercase
            - Whitespaces replaced with underscores
            - Special characters like dashes, equals signs, asterisks, commas, parentheses, and accents are removed or replaced.

        Example
        -------
        >>> clean_name("José-Álvaro (example)=name*")
        "jose_alvaro_example_name"
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

    def clean_awards_by_year(self, fiscal_year: int) -> pl.DataFrame:
        empty_df = [
            pl.Series("assistance_transaction_unique_key", [], dtype=pl.Utf8),
            pl.Series("assistance_award_unique_key", [], dtype=pl.Utf8),
            pl.Series("award_id_fain", [], dtype=pl.Utf8),
            pl.Series("modification_number", [], dtype=pl.Utf8),
            pl.Series("award_id_uri", [], dtype=pl.Utf8),
            pl.Series("sai_number", [], dtype=pl.Utf8),
            pl.Series("federal_action_obligation", [], dtype=pl.Float64),
            pl.Series("total_obligated_amount", [], dtype=pl.Float64),
            pl.Series("total_outlayed_amount_for_overall_award", [], dtype=pl.Float64),
            pl.Series("indirect_cost_federal_share_amount", [], dtype=pl.Float64),
            pl.Series("non_federal_funding_amount", [], dtype=pl.Float64),
            pl.Series("total_non_federal_funding_amount", [], dtype=pl.Float64),
            pl.Series("face_value_of_loan", [], dtype=pl.Float64),
            pl.Series("original_loan_subsidy_cost", [], dtype=pl.Float64),
            pl.Series("total_face_value_of_loan", [], dtype=pl.Float64),
            pl.Series("total_loan_subsidy_cost", [], dtype=pl.Float64),
            pl.Series("generated_pragmatic_obligations", [], dtype=pl.Float64),
            pl.Series(
                "disaster_emergency_fund_codes_for_overall_award", [], dtype=pl.Utf8
            ),
            pl.Series(
                "outlayed_amount_from_COVID-19_supplementals_for_overall_award",
                [],
                dtype=pl.Float64,
            ),
            pl.Series(
                "obligated_amount_from_COVID-19_supplementals_for_overall_award",
                [],
                dtype=pl.Float64,
            ),
            pl.Series(
                "outlayed_amount_from_IIJA_supplemental_for_overall_award",
                [],
                dtype=pl.Float64,
            ),
            pl.Series(
                "obligated_amount_from_IIJA_supplemental_for_overall_award",
                [],
                dtype=pl.Float64,
            ),
            pl.Series("action_date", [], dtype=pl.Date),
            pl.Series("action_date_fiscal_year", [], dtype=pl.Int64),
            pl.Series("period_of_performance_start_date", [], dtype=pl.Date),
            pl.Series("period_of_performance_current_end_date", [], dtype=pl.Date),
            pl.Series("awarding_agency_code", [], dtype=pl.Utf8),
            pl.Series("awarding_agency_name", [], dtype=pl.Utf8),
            pl.Series("awarding_sub_agency_code", [], dtype=pl.Utf8),
            pl.Series("awarding_sub_agency_name", [], dtype=pl.Utf8),
            pl.Series("awarding_office_code", [], dtype=pl.Utf8),
            pl.Series("awarding_office_name", [], dtype=pl.Utf8),
            pl.Series("funding_agency_code", [], dtype=pl.Utf8),
            pl.Series("funding_agency_name", [], dtype=pl.Utf8),
            pl.Series("funding_sub_agency_code", [], dtype=pl.Utf8),
            pl.Series("funding_sub_agency_name", [], dtype=pl.Utf8),
            pl.Series("funding_office_code", [], dtype=pl.Utf8),
            pl.Series("funding_office_name", [], dtype=pl.Utf8),
            pl.Series("treasury_accounts_funding_this_award", [], dtype=pl.Utf8),
            pl.Series("federal_accounts_funding_this_award", [], dtype=pl.Utf8),
            pl.Series("object_classes_funding_this_award", [], dtype=pl.Utf8),
            pl.Series("program_activities_funding_this_award", [], dtype=pl.Utf8),
            pl.Series("recipient_uei", [], dtype=pl.Utf8),
            pl.Series("recipient_duns", [], dtype=pl.Utf8),
            pl.Series("recipient_name", [], dtype=pl.Utf8),
            pl.Series("recipient_name_raw", [], dtype=pl.Utf8),
            pl.Series("recipient_parent_uei", [], dtype=pl.Utf8),
            pl.Series("recipient_parent_duns", [], dtype=pl.Utf8),
            pl.Series("recipient_parent_name", [], dtype=pl.Utf8),
            pl.Series("recipient_parent_name_raw", [], dtype=pl.Utf8),
            pl.Series("recipient_country_code", [], dtype=pl.Utf8),
            pl.Series("recipient_country_name", [], dtype=pl.Utf8),
            pl.Series("recipient_address_line_1", [], dtype=pl.Utf8),
            pl.Series("recipient_address_line_2", [], dtype=pl.Utf8),
            pl.Series("recipient_city_code", [], dtype=pl.Utf8),
            pl.Series("recipient_city_name", [], dtype=pl.Utf8),
            pl.Series(
                "prime_award_transaction_recipient_county_fips_code", [], dtype=pl.Utf8
            ),
            pl.Series("recipient_county_name", [], dtype=pl.Utf8),
            pl.Series(
                "prime_award_transaction_recipient_state_fips_code", [], dtype=pl.Utf8
            ),
            pl.Series("recipient_state_code", [], dtype=pl.Utf8),
            pl.Series("recipient_state_name", [], dtype=pl.Utf8),
            pl.Series("recipient_zip_code", [], dtype=pl.Utf8),
            pl.Series("recipient_zip_last_4_code", [], dtype=pl.Utf8),
            pl.Series(
                "prime_award_transaction_recipient_cd_original", [], dtype=pl.Utf8
            ),
            pl.Series(
                "prime_award_transaction_recipient_cd_current", [], dtype=pl.Utf8
            ),
            pl.Series("recipient_foreign_city_name", [], dtype=pl.Utf8),
            pl.Series("recipient_foreign_province_name", [], dtype=pl.Utf8),
            pl.Series("recipient_foreign_postal_code", [], dtype=pl.Utf8),
            pl.Series("primary_place_of_performance_scope", [], dtype=pl.Utf8),
            pl.Series("primary_place_of_performance_country_code", [], dtype=pl.Utf8),
            pl.Series("primary_place_of_performance_country_name", [], dtype=pl.Utf8),
            pl.Series("primary_place_of_performance_code", [], dtype=pl.Utf8),
            pl.Series("primary_place_of_performance_city_name", [], dtype=pl.Utf8),
            pl.Series(
                "prime_award_transaction_place_of_performance_county_fips_code",
                [],
                dtype=pl.Utf8,
            ),
            pl.Series("primary_place_of_performance_county_name", [], dtype=pl.Utf8),
            pl.Series(
                "prime_award_transaction_place_of_performance_state_fips_code",
                [],
                dtype=pl.Utf8,
            ),
            pl.Series("primary_place_of_performance_state_name", [], dtype=pl.Utf8),
            pl.Series("primary_place_of_performance_zip_4", [], dtype=pl.Utf8),
            pl.Series(
                "prime_award_transaction_place_of_performance_cd_original",
                [],
                dtype=pl.Utf8,
            ),
            pl.Series(
                "prime_award_transaction_place_of_performance_cd_current",
                [],
                dtype=pl.Utf8,
            ),
            pl.Series(
                "primary_place_of_performance_foreign_location", [], dtype=pl.Utf8
            ),
            pl.Series("cfda_number", [], dtype=pl.Utf8),
            pl.Series("cfda_title", [], dtype=pl.Utf8),
            pl.Series("funding_opportunity_number", [], dtype=pl.Utf8),
            pl.Series("funding_opportunity_goals_text", [], dtype=pl.Utf8),
            pl.Series("assistance_type_code", [], dtype=pl.Utf8),
            pl.Series("assistance_type_description", [], dtype=pl.Utf8),
            pl.Series("transaction_description", [], dtype=pl.Utf8),
            pl.Series("prime_award_base_transaction_description", [], dtype=pl.Utf8),
            pl.Series("business_funds_indicator_code", [], dtype=pl.Utf8),
            pl.Series("business_funds_indicator_description", [], dtype=pl.Utf8),
            pl.Series("business_types_code", [], dtype=pl.Utf8),
            pl.Series("business_types_description", [], dtype=pl.Utf8),
            pl.Series("correction_delete_indicator_code", [], dtype=pl.Utf8),
            pl.Series("correction_delete_indicator_description", [], dtype=pl.Utf8),
            pl.Series("action_type_code", [], dtype=pl.Utf8),
            pl.Series("action_type_description", [], dtype=pl.Utf8),
            pl.Series("record_type_code", [], dtype=pl.Utf8),
            pl.Series("record_type_description", [], dtype=pl.Utf8),
            pl.Series("highly_compensated_officer_1_name", [], dtype=pl.Utf8),
            pl.Series("highly_compensated_officer_1_amount", [], dtype=pl.Float64),
            pl.Series("highly_compensated_officer_2_name", [], dtype=pl.Utf8),
            pl.Series("highly_compensated_officer_2_amount", [], dtype=pl.Float64),
            pl.Series("highly_compensated_officer_3_name", [], dtype=pl.Utf8),
            pl.Series("highly_compensated_officer_3_amount", [], dtype=pl.Float64),
            pl.Series("highly_compensated_officer_4_name", [], dtype=pl.Utf8),
            pl.Series("highly_compensated_officer_4_amount", [], dtype=pl.Float64),
            pl.Series("highly_compensated_officer_5_name", [], dtype=pl.Utf8),
            pl.Series("highly_compensated_officer_5_amount", [], dtype=pl.Float64),
            pl.Series("usaspending_permalink", [], dtype=pl.Utf8),
            pl.Series("initial_report_date", [], dtype=pl.Date),
            pl.Series("last_modified_date", [], dtype=pl.Date),
            pl.Series("fiscal_year", [], dtype=pl.Int64),
        ]

        acs = pl.DataFrame(empty_df).clear()

        data_directory = "data/raw"
        local_zip_path = os.path.join(data_directory, f"{fiscal_year}_spending.csv")

        df = pl.read_csv(local_zip_path, infer_schema_length=10000)

        df = df.with_columns(
            pl.lit(fiscal_year).alias("fiscal_year").cast(pl.Int64),
        )

        df = df.with_columns(
            [
                pl.col("assistance_transaction_unique_key").cast(pl.Utf8),
                pl.col("assistance_award_unique_key").cast(pl.Utf8),
                pl.col("award_id_fain").cast(pl.Utf8),
                pl.col("modification_number").cast(pl.Utf8),
                pl.col("award_id_uri").cast(pl.Utf8),
                pl.col("sai_number").cast(pl.Utf8),
                pl.col("federal_action_obligation").cast(pl.Float64),
                pl.col("total_obligated_amount").cast(pl.Float64),
                pl.col("total_outlayed_amount_for_overall_award").cast(pl.Float64),
                pl.col("indirect_cost_federal_share_amount").cast(pl.Float64),
                pl.col("non_federal_funding_amount").cast(pl.Float64),
                pl.col("total_non_federal_funding_amount").cast(pl.Float64),
                pl.col("face_value_of_loan").cast(pl.Float64),
                pl.col("original_loan_subsidy_cost").cast(pl.Float64),
                pl.col("total_face_value_of_loan").cast(pl.Float64),
                pl.col("total_loan_subsidy_cost").cast(pl.Float64),
                pl.col("generated_pragmatic_obligations").cast(pl.Float64),
                pl.col("disaster_emergency_fund_codes_for_overall_award").cast(pl.Utf8),
                pl.col(
                    "outlayed_amount_from_COVID-19_supplementals_for_overall_award"
                ).cast(pl.Float64),
                pl.col(
                    "obligated_amount_from_COVID-19_supplementals_for_overall_award"
                ).cast(pl.Float64),
                pl.col("outlayed_amount_from_IIJA_supplemental_for_overall_award").cast(
                    pl.Float64
                ),
                pl.col(
                    "obligated_amount_from_IIJA_supplemental_for_overall_award"
                ).cast(pl.Float64),
                pl.col("action_date").str.strptime(pl.Date, format="%Y-%m-%d"),
                pl.col("action_date_fiscal_year").cast(pl.Int64),
                pl.col("period_of_performance_start_date").str.strptime(
                    pl.Date, format="%Y-%m-%d"
                ),
                pl.col("period_of_performance_current_end_date").str.strptime(
                    pl.Date, format="%Y-%m-%d"
                ),
                pl.col("awarding_agency_code").cast(pl.Utf8),
                pl.col("awarding_agency_name").cast(pl.Utf8),
                pl.col("awarding_sub_agency_code").cast(pl.Utf8),
                pl.col("awarding_sub_agency_name").cast(pl.Utf8),
                pl.col("awarding_office_code").cast(pl.Utf8),
                pl.col("awarding_office_name").cast(pl.Utf8),
                pl.col("funding_agency_code").cast(pl.Utf8),
                pl.col("funding_agency_name").cast(pl.Utf8),
                pl.col("funding_sub_agency_code").cast(pl.Utf8),
                pl.col("funding_sub_agency_name").cast(pl.Utf8),
                pl.col("funding_office_code").cast(pl.Utf8),
                pl.col("funding_office_name").cast(pl.Utf8),
                pl.col("treasury_accounts_funding_this_award").cast(pl.Utf8),
                pl.col("federal_accounts_funding_this_award").cast(pl.Utf8),
                pl.col("object_classes_funding_this_award").cast(pl.Utf8),
                pl.col("program_activities_funding_this_award").cast(pl.Utf8),
                pl.col("recipient_uei").cast(pl.Utf8),
                pl.col("recipient_duns").cast(pl.Utf8),
                pl.col("recipient_name").cast(pl.Utf8),
                pl.col("recipient_name_raw").cast(pl.Utf8),
                pl.col("recipient_parent_uei").cast(pl.Utf8),
                pl.col("recipient_parent_duns").cast(pl.Utf8),
                pl.col("recipient_parent_name").cast(pl.Utf8),
                pl.col("recipient_parent_name_raw").cast(pl.Utf8),
                pl.col("recipient_country_code").cast(pl.Utf8),
                pl.col("recipient_country_name").cast(pl.Utf8),
                pl.col("recipient_address_line_1").cast(pl.Utf8),
                pl.col("recipient_address_line_2").cast(pl.Utf8),
                pl.col("recipient_city_code").cast(pl.Utf8),
                pl.col("recipient_city_name").cast(pl.Utf8),
                pl.col("prime_award_transaction_recipient_county_fips_code").cast(
                    pl.Utf8
                ),
                pl.col("recipient_county_name").cast(pl.Utf8),
                pl.col("prime_award_transaction_recipient_state_fips_code").cast(
                    pl.Utf8
                ),
                pl.col("recipient_state_code").cast(pl.Utf8),
                pl.col("recipient_state_name").cast(pl.Utf8),
                pl.col("recipient_zip_code").cast(pl.Utf8),
                pl.col("recipient_zip_last_4_code").cast(pl.Utf8),
                pl.col("prime_award_transaction_recipient_cd_original").cast(pl.Utf8),
                pl.col("prime_award_transaction_recipient_cd_current").cast(pl.Utf8),
                pl.col("recipient_foreign_city_name").cast(pl.Utf8),
                pl.col("recipient_foreign_province_name").cast(pl.Utf8),
                pl.col("recipient_foreign_postal_code").cast(pl.Utf8),
                pl.col("primary_place_of_performance_scope").cast(pl.Utf8),
                pl.col("primary_place_of_performance_country_code").cast(pl.Utf8),
                pl.col("primary_place_of_performance_country_name").cast(pl.Utf8),
                pl.col("primary_place_of_performance_code").cast(pl.Utf8),
                pl.col("primary_place_of_performance_city_name").cast(pl.Utf8),
                pl.col(
                    "prime_award_transaction_place_of_performance_county_fips_code"
                ).cast(pl.Utf8),
                pl.col("primary_place_of_performance_county_name").cast(pl.Utf8),
                pl.col(
                    "prime_award_transaction_place_of_performance_state_fips_code"
                ).cast(pl.Utf8),
                pl.col("primary_place_of_performance_state_name").cast(pl.Utf8),
                pl.col("primary_place_of_performance_zip_4").cast(pl.Utf8),
                pl.col("prime_award_transaction_place_of_performance_cd_original").cast(
                    pl.Utf8
                ),
                pl.col("prime_award_transaction_place_of_performance_cd_current").cast(
                    pl.Utf8
                ),
                pl.col("primary_place_of_performance_foreign_location").cast(pl.Utf8),
                pl.col("cfda_number").cast(pl.Utf8),
                pl.col("cfda_title").cast(pl.Utf8),
                pl.col("funding_opportunity_number").cast(pl.Utf8),
                pl.col("funding_opportunity_goals_text").cast(pl.Utf8),
                pl.col("assistance_type_code").cast(pl.Utf8),
                pl.col("assistance_type_description").cast(pl.Utf8),
                pl.col("transaction_description").cast(pl.Utf8),
                pl.col("prime_award_base_transaction_description").cast(pl.Utf8),
                pl.col("business_funds_indicator_code").cast(pl.Utf8),
                pl.col("business_funds_indicator_description").cast(pl.Utf8),
                pl.col("business_types_code").cast(pl.Utf8),
                pl.col("business_types_description").cast(pl.Utf8),
                pl.col("correction_delete_indicator_code").cast(pl.Utf8),
                pl.col("correction_delete_indicator_description").cast(pl.Utf8),
                pl.col("action_type_code").cast(pl.Utf8),
                pl.col("action_type_description").cast(pl.Utf8),
                pl.col("record_type_code").cast(pl.Utf8),
                pl.col("record_type_description").cast(pl.Utf8),
                pl.col("highly_compensated_officer_1_name").cast(pl.Utf8),
                pl.col("highly_compensated_officer_1_amount").cast(pl.Float64),
                pl.col("highly_compensated_officer_2_name").cast(pl.Utf8),
                pl.col("highly_compensated_officer_2_amount").cast(pl.Float64),
                pl.col("highly_compensated_officer_3_name").cast(pl.Utf8),
                pl.col("highly_compensated_officer_3_amount").cast(pl.Float64),
                pl.col("highly_compensated_officer_4_name").cast(pl.Utf8),
                pl.col("highly_compensated_officer_4_amount").cast(pl.Float64),
                pl.col("highly_compensated_officer_5_name").cast(pl.Utf8),
                pl.col("highly_compensated_officer_5_amount").cast(pl.Float64),
                pl.col("usaspending_permalink").cast(pl.Utf8),
                pl.col("initial_report_date").str.strptime(pl.Date, format="%Y-%m-%d"),
                pl.col("last_modified_date").str.strptime(pl.Date, format="%Y-%m-%d"),
                pl.col("fiscal_year").cast(pl.Int64),
            ]
        )

        acs = pl.concat([acs, df], how="vertical")
        logging.info(f"Cleaned data for fiscal year {fiscal_year}.")

        acs = acs.with_columns(
            [
                pl.col("action_date").cast(pl.Utf8).fill_null(pl.lit(None)),
                pl.col("period_of_performance_start_date")
                .cast(pl.Utf8)
                .fill_null(pl.lit(None)),
                pl.col("period_of_performance_current_end_date")
                .cast(pl.Utf8)
                .fill_null(pl.lit(None)),
                pl.col("initial_report_date").cast(pl.Utf8).fill_null(pl.lit(None)),
                pl.col("last_modified_date").cast(pl.Utf8).fill_null(pl.lit(None)),
            ]
        )
        acs = acs.rename({col: col.lower().replace("-", "_") for col in acs.columns})

        return acs
    
    def download_with_retry(self, url, file_path):
        TARGET_HTML_SIZE = 3893

        while True:
            self.pull_file(url, file_path)
            file_size = os.path.getsize(file_path)

            if file_size != TARGET_HTML_SIZE:
                logging.info(f"✅ Downloaded {file_path} with size {file_size} bytes")
                break
            else:
                logging.info(f"⚠️ File not ready for download, retrying in 30 seconds...")
                os.remove(file_path)
                time.sleep(30)

    def pull_awards_by_year(self, fiscal_year: int) -> pl.DataFrame:
        base_url = "https://api.usaspending.gov/api/v2/bulk_download/awards/"
        headers = {"Content-Type": "application/json"}

        payload = {
            "filters": {
                "prime_award_types": ["02", "03", "04", "05"],
                "date_type": "action_date",
                "date_range": {
                    "start_date": f"{fiscal_year - 1}-10-01",
                    "end_date": f"{fiscal_year}-09-30",
                },
                "place_of_performance_locations": [{"country": "USA", "state": "PR"}],
            },
            "subawards": False,
            "order": "desc",
            "sort": "total_obligated_amount",
            "file_format": "csv",
        }

        try:
            logging.info(f"Downloading file for fiscal year {fiscal_year}.")
            response = requests.post(
                base_url, json=payload, headers=headers, timeout=None
            )

            if response.status_code == 200:
                response = response.json()
                url = response.get("file_url")
                if not url:
                    return pl.DataFrame()
                logging.info(f"Downloaded file for fiscal year: {fiscal_year}.")
                file_path = f"data/raw/{fiscal_year}_spending.zip"
                self.download_with_retry(url, file_path)
                self.extract_awards_by_year(fiscal_year)
                df = self.clean_awards_by_year(fiscal_year)
                return df

            else:
                logging.error(
                    f"Error en la solicitud: {response.status_code}, {response.reason}"
                )

        except Exception as e:
            logging.error(f"Error al realizar la solicitud: {e}")

    def extract_awards_by_year(self, year: int):
        extracted = False
        local_zip_path = f"{self.saving_dir}raw/{year}_spending.zip"
        with zipfile.ZipFile(local_zip_path, "r") as zip_ref:
            zip_ref.extractall(f"{self.saving_dir}/raw")
            logging.info("Extracted file.")
            extracted = True
        extracted_files = [
            f for f in os.listdir(f"{self.saving_dir}/raw") if f.endswith(".csv")
        ]
        if extracted:
            latest_file = max(
                extracted_files,
                key=lambda f: os.path.getmtime(
                    os.path.join(f"{self.saving_dir}/raw", f)
                ),
            )
            new_name = f"{year}_spending.csv"
            old_path = os.path.join(f"{self.saving_dir}/raw", latest_file)
            new_path = os.path.join(f"{self.saving_dir}/raw", new_name)
            os.rename(old_path, new_path)
        else:
            logging.info("No extracted files found.")
        return None

    def insert_awards_by_year(self, fiscal_year):
        if "AwardTable" not in self.conn.sql("SHOW TABLES;").df().get("name").tolist():
            init_awards_table(self.data_file)

        try:
            result = self.conn.sql(
                f"SELECT COUNT(*) FROM AwardTable WHERE fiscal_year = {fiscal_year}"
            )
            if result.fetchone()[0] == 0:
                df = self.pull_awards_by_year(fiscal_year)
                if df.is_empty():
                    return self.conn.sql("SELECT * FROM 'AwardTable';").pl()
                self.conn.sql("INSERT INTO 'AwardTable' BY NAME SELECT * FROM df;")
                logging.info(f"Inserted fiscal year {fiscal_year} to sqlite table.")
            else:
                logging.info(f"Fiscal year {fiscal_year} already in db.")
        except Exception as e:
            logging.error(
                f"Error inserting fiscal year {fiscal_year} to sqlite table. {e}"
            )
            return self.conn.sql("SELECT * FROM 'AwardTable';").pl()
        return self.conn.sql("SELECT * FROM 'AwardTable';").pl()

    def clean_energy_df(self) -> pl.DataFrame:
        input_csv_path = f"{self.saving_dir}/raw/aee-meta-ultimo.csv"
        text_col       = "mes"
        pdf = pd.read_csv(input_csv_path, encoding="latin1", dtype=str)

        def clean_name(col: str) -> str:
            col = col.lower()
            col = re.sub(r"\s+", " ", col)
            col = col.replace("/", "_")
            for old, new in [
                ("(", ""), (")", ""),
                ("$", "dollar"), ("¢", "cent"),
                ("#", "amount"), ("%", "porcentage"),
                ("ó", "o"), ("á", "a"), ("é", "e"), ("í", "i"), ("ú", "u"),
            ]:
                col = col.replace(old, new)
            col = col.replace("-", "_").replace(" ", "_")
            col = re.sub(r"_+", "_", col)
            return col.strip("_")

        orig_cols    = pdf.columns.to_list()
        cleaned_cols = [clean_name(c) for c in orig_cols]
        pdf.columns  = cleaned_cols

        df = pl.from_pandas(pdf)
        exprs = []
        cols_to_process = list(df.columns[:-1])

        for col in cols_to_process:
            cleaned = (
                pl.col(col)
                    .str.replace_all(",", "")
                    .str.replace_all(r"^\s+|\s+$", "")
            )
            if col == text_col:
                expr = (
                    pl.when(cleaned.is_in(["", "-", None]))
                        .then(None)
                        .otherwise(cleaned)
                ).alias(col)
            else:
                expr = (
                    pl.when(cleaned.is_in(["", "-", None]))
                        .then(None)
                        .otherwise(cleaned)
                ).cast(pl.Float64).alias(col)
            exprs.append(expr)

        df_clean = df.select(exprs)

        return df_clean


    def insert_energy_data(self):
        existing = (
            self.conn
                .sql("SHOW TABLES;")
                .df()
                .get("name")
                .tolist()
        )
        if "EnergyTable" not in existing:
            init_energy_table(self.data_file)

        try:
            count = self.conn \
                            .sql("SELECT COUNT(*) FROM EnergyTable;") \
                            .fetchone()[0]
            if count == 0:
                df_clean = self.clean_energy_df()
                self.conn.register("tmp_energy", df_clean)
                self.conn.execute("""
                    INSERT INTO EnergyTable
                    SELECT * FROM tmp_energy;
                """)
                logging.info("Inserted cleaned energy data into EnergyTable (via Polars in RAM).")
            else:
                logging.info("EnergyTable already contains data; skipping load.")
        except Exception as e:
            logging.error(f"Error inserting energy data: {e}")
            return self.conn.sql("SELECT * FROM EnergyTable;").pl()

        return self.conn.sql("SELECT * FROM EnergyTable;").pl()

    def pull_energy_data(self, update: bool = False):
        url = "https://indicadores.pr/dataset/49746389-12ce-48f6-b578-65f6dc46f53f/resource/8025f821-45c1-4c6a-b2f4-8d641cc03df1/download/aee-meta-ultimo.csv"
        file_path = f"{self.saving_dir}/raw/aee-meta-ultimo.csv"
        if os.path.exists(file_path) and not update:
            logging.info(f"[DataPull] {file_path} ya existe — omito descarga.")
            return file_path    
        
        self.pull_file(url, file_path, False)
        logging.info(f"Downloaded file to {file_path}")
        return file_path

    def pull_consumer(self, file_path: str):
        """
        Downloads a file from a specific URL using a POST request to simulate a form submission.

        This method handles retries on failures, logs the progress of the download, and saves the file
        to the specified path. It uses a session with custom headers and form data to ensure the correct
        interaction with the target website, including handling the `__VIEWSTATE` and other form parameters.

        Parameters:
        ----------
        file_path : str
            The local path (including the filename) where the downloaded file will be saved.
            If the directory doesn't exist, the function will log an error.

        Returns:
        -------
        None

        Side Effects:
        --------------
        - Logs the download progress using `tqdm`.
        - Logs a message indicating success or failure of the download.
        - In case of failure, logs the HTTP status code or exception encountered.

        Exceptions:
        -----------
        - Raises `requests.exceptions.RequestException` if there is an issue with the HTTP request.

        Example:
        --------
        pull_consumer("path/to/save/file.zip")
        """
        session = requests.Session()
        retry = Retry(
            total=5,  # Number of retries
            backoff_factor=1,  # Wait 1s, 2s, 4s, etc., between retries
            status_forcelist=[500, 502, 503, 504],  # Retry on these status codes
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)

        headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:138.0) Gecko/20100101 Firefox/138.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            # 'Accept-Encoding': 'gzip, deflate, br, zstd',
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": "https://www.mercadolaboral.pr.gov",
            "DNT": "1",
            "Sec-GPC": "1",
            "Connection": "keep-alive",
            "Referer": "https://www.mercadolaboral.pr.gov/Tablas_Estadisticas/Otras_Tablas/T_Indice_Precio.aspx",
            # 'Cookie': 'ASP.NET_SessionId=xo15dko1bo2xpx3z2v00hbvd',
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-User": "?1",
            "Priority": "u=0, i",
        }
        data = {
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": "/wEPDwUKLTcxODY5NDM5NGQYAQUeX19Db250cm9sc1JlcXVpcmVQb3N0QmFja0tleV9fFgQFEmN0bDAwJEltYWdlQnV0dG9uMgUSY3RsMDAkSW1hZ2VCdXR0b243BRJjdGwwMCRJbWFnZUJ1dHRvbjYFE2N0bDAwJEltYWdlQnV0dG9uMjlIO3VA46VF5zXGzqrFyKJq/pfxyKZMac4irRIb4DpXIQ==",
            "__VIEWSTATEGENERATOR": "C7F80305",
            "__PREVIOUSPAGE": "VKahnVo012KjGbdlSp5RK6wQUNIj7ME4Z2HM5zGucZzQH-dTOPAP5jfmExop3K6Pm5Z7xxAH8VVdQUs6TdSmfUqv3Z61DnX526bDb7jEq_ikwxvQYi_3x2CCIUuSOgz-iXXdL0bCH31B2Re2nUSuOw2",
            "__EVENTVALIDATION": "/wEdAAnf8EUjtDskqQHefeamGW3JK54MsQ9Z5Tipa4C3CU9lIy6KqsTtzWiK229TcIgvoTmJ5D8KsXArXsSdeMqOt6pk+d3fBy3LDDz0lsNt4u+CuDIENRTx3TqpeEC0BFNcbx18XLv2PDpbcvrQF1sPng9RHC+hNwNMKsAjTYpq3ZLON4FBZYDVNXrnB/9WmjDFKj5xBappsykmZwHNQiZ7w2z/NADeSIbXMxYQcPGyp1PO/Q==",
            "ctl00$MainContent$Button1": "Descargar",
        }

        # Perform the POST request to download the file
        response = session.post(
            "https://www.mercadolaboral.pr.gov/Tablas_Estadisticas/Otras_Tablas/T_Indice_Precio.aspx",
            headers=headers,
            data=data,
            stream=True,  # Stream the response to handle large files
        )

        # Check if the request was successful
        if response.status_code == 200:
            # Get the total file size from the headers
            total_size = int(response.headers.get("content-length", 0))
            # Open the file for writing in binary mode
            with open(file_path, "wb") as file:
                # Use tqdm to show the download progress
                for chunk in tqdm(
                    response.iter_content(chunk_size=8192),
                    total=total_size // 8192,
                    unit="KB",
                    desc="Downloading",
                ):
                    if chunk:  # Filter out keep-alive new chunks
                        file.write(chunk)
            logging.info(f"Downloaded file to {file_path}")
        else:
            logging.error(f"Failed to download file: {response.status_code}")

    def pull_activity(self, file_path: str):
        """
        Downloads an Excel file containing activity data from a specific URL.

        This method calls the `pull_file` method to download the file from the given URL and saves
        it to the specified file path. After the download completes, a log message is generated
        confirming the file's location.

        Parameters:
        ----------
        file_path : str
            The local path (including the filename) where the downloaded file will be saved.
            If the directory doesn't exist, it should be handled before calling the method.

        Returns:
        -------
        None

        Side Effects:
        --------------
        - Calls the `pull_file` method to perform the actual download.
        - Logs the download progress via `logging.info()` upon successful completion.

        Example:
        --------
        pull_activity("path/to/save/activity_data.xls")
        """
        url = "https://www.bde.pr.gov/BDE/PREDDOCS/I_EAI.XLS"
        self.pull_file(url, file_path)
        logging.info(f"Downloaded file to {file_path}")

    def pull_file(self, url: str, filename: str, verify: bool = True) -> None:
        """
        Downloads a file from a specified URL and saves it to the given local filename.

        This method streams the file from the provided URL, writes it to the local file,
        and shows a progress bar using the `tqdm` library to track download progress.
        If the file already exists at the given location, the download is skipped.

        Parameters:
        ----------
        url : str
            The URL from which the file will be downloaded.

        filename : str
            The local path (including the filename) where the downloaded file will be saved.
            If the file already exists, the download is skipped.

        verify : bool, optional, default=True
            Whether to verify the SSL certificate of the remote server.
            If set to False, the SSL certificate validation will be skipped.
            This is useful when downloading from servers with invalid or self-signed certificates.

        Returns:
        -------
        None

        Side Effects:
        --------------
        - Downloads the file from the specified URL.
        - Saves the file to the given filename.
        - Logs an info message when the download is either skipped or successfully completed.
        - Displays a progress bar during the download.

        Example:
        --------
        pull_file("https://example.com/data.zip", "local_data.zip")
        """
        if os.path.exists(filename):
            logging.info(f"File {filename} already exists, skipping download")
        else:
            chunk_size = 10 * 1024 * 1024

            with requests.get(url, stream=True, verify=verify) as response:
                total_size = int(response.headers.get("content-length", 0))

                with tqdm(
                    total=total_size,
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                    desc="Downloading",
                ) as bar:
                    with open(filename, "wb") as file:
                        for chunk in response.iter_content(chunk_size=chunk_size):
                            if chunk:
                                file.write(chunk)
                                bar.update(
                                    len(chunk)
                                )  # Update the progress bar with the size of the chunk
                logging.info(f"Downloaded {filename}")

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
            url = "https://jp.pr.gov/wp-content/uploads/2024/09/Indicadores_Economicos_9.13.2024.xlsx"
            self.pull_file(url, f"{self.saving_dir}raw/economic_indicators.xlsx")
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

            jp_df = jp_df.with_columns(
                year=pl.col("date").dt.year(), month=pl.col("date").dt.month()
            )
            jp_df = jp_df.with_columns(
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
            url = "https://jp.pr.gov/wp-content/uploads/2024/09/Indicadores_Economicos_9.13.2024.xlsx"
            self.pull_file(url, f"{self.saving_dir}raw/economic_indicators.xlsx")
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

            jp_df = jp_df.with_columns(
                year=pl.col("date").dt.year(), month=pl.col("date").dt.month()
            )
            jp_df = jp_df.with_columns(
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

    def pull_energy_data(self):
        url = "https://indicadores.pr/dataset/49746389-12ce-48f6-b578-65f6dc46f53f/resource/8025f821-45c1-4c6a-b2f4-8d641cc03df1/download/aee-meta-ultimo.csv"
        file_path = f"{self.saving_dir}/raw/aee-meta-ultimo.csv"
        self.pull_file(url, file_path, False)
        logging.info(f"Downloaded file to {file_path}")

    def process_awards_by_secter(self, type, agency):
        df = self.conn.sql(f"SELECT * FROM AwardTable;").pl()
        agency_list = df.select("awarding_agency_name").unique().to_series().to_list()

        month_map = {
            1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun', 
            7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec', 
        }
        months = list(month_map.values())
        
        df = df.with_columns([
            pl.col("action_date").str.strptime(pl.Date, "%Y-%m-%d").alias("parsed_date"),
            ((pl.col("action_date").str.strptime(pl.Date, "%Y-%m-%d").dt.month()).alias("month")),
            ((pl.col("action_date").str.strptime(pl.Date, "%Y-%m-%d").dt.year()).alias("year"))
        ])
        df = df.with_columns(
            pl.col("month").cast(pl.String).replace(month_map).alias("month_name").cast(pl.String),
            (pl.col("year") + (pl.col("month") > 6).cast(pl.Int32)).alias("pr_fiscal_year"),
            pl.col('awarding_agency_name').str.to_lowercase().str.replace_all(" ", "_")
        )
        agency = agency.lower()
        type = type.lower()

        agg_expr = "federal_action_obligation"
        df = df.filter(pl.col("awarding_agency_name") == agency)

        match type:
            case 'fiscal':
                grouped_df = df.with_columns(
                    (pl.col("pr_fiscal_year")).cast(pl.String).alias("time_period")
                )
                grouped_df = grouped_df.group_by(['time_period', 'awarding_agency_name']).agg(pl.col(agg_expr).sum())
            case 'yearly':
                grouped_df = df.with_columns(
                    (pl.col("year")).cast(pl.String).alias("time_period")
                )
                grouped_df = grouped_df.group_by(['time_period', 'awarding_agency_name']).agg(pl.col(agg_expr).sum())
            case 'quarterly':
                quarter_expr = (
                    pl.when(pl.col("month").is_in([1, 2, 3])).then(pl.lit("q1"))
                    .when(pl.col("month").is_in([4, 5, 6])).then(pl.lit("q2"))
                    .when(pl.col("month").is_in([7, 8, 9])).then(pl.lit("q3"))
                    .when(pl.col("month").is_in([10, 11, 12])).then(pl.lit("q4"))
                    .otherwise(pl.lit("q?"))
                )
                grouped_df = df.with_columns(
                    (pl.col("year").cast(pl.String) + quarter_expr).alias("time_period")
                )
                grouped_df = grouped_df.group_by(['time_period', 'awarding_agency_name']).agg(pl.col(agg_expr).sum())
            case 'monthly':
                results = pl.DataFrame(schema={
                    "month_name": pl.String,
                    "awarding_agency_name": pl.String,
                    "year": pl.Int32,
                    "federal_action_obligation": pl.Float32,
                    "time_period": pl.String,
                })
                months = pl.DataFrame({'month_name': months}).select([
                    pl.col("month_name").cast(pl.String)
                ])
                for year in df.select(pl.col("year")).unique().to_series().to_list():
                    df_year = df.filter(pl.col("year") == year)
                    df_year = months.join(df_year, on="month_name", how="outer")
                    df_year = df_year.select(["month_name", "federal_action_obligation", "awarding_agency_name", "year"]).with_columns(
                        pl.col('year').fill_null(year),
                        pl.col("federal_action_obligation").fill_null(0),
                        pl.col('awarding_agency_name').fill_null(agency)
                    )
                    df_year = df_year.group_by(['month_name', 'awarding_agency_name', 'year']).agg(pl.col(agg_expr).sum())
                    df_year = df_year.with_columns(
                        (pl.col("year").cast(pl.Utf8) + pl.col("month_name")).alias("time_period")
                    )
                    results = pl.concat([results, df_year])
                grouped_df = results
                grouped_df = grouped_df.group_by(['awarding_agency_name', 'time_period']).agg(pl.col(agg_expr).sum())
                grouped_df = grouped_df.with_columns(
                    pl.col("time_period").str.strptime(pl.Date, "%Y%b", strict=False).dt.strftime("%Y-%m").alias("parsed_period")
                ).sort("parsed_period")
        
        return grouped_df, agency_list
    
    def process_awards_by_category(self, year, quarter, month, type, category):
        df = self.conn.sql(f"SELECT * FROM AwardTable;").pl()

        excluded_columns = ["federal_action_obligation", "fiscal_year", "action_date"]
        columns = [
            {"value": col, "label": col.replace("_", " ").capitalize()}
            for col in df.columns
            if col not in excluded_columns and "date" not in col.lower()
        ]
        
        df = df.with_columns([
            pl.col("action_date").str.strptime(pl.Date, "%Y-%m-%d").alias("parsed_date"),
            (pl.col("action_date").str.strptime(pl.Date, "%Y-%m-%d").dt.month()).alias("month"),
            (pl.col("action_date").str.strptime(pl.Date, "%Y-%m-%d").dt.year()).alias("year"),
            pl.col(category).str.to_lowercase()
        ])
        df = df.with_columns([
            (pl.col("year") + (pl.col("month") > 6).cast(pl.Int32)).alias("pr_fiscal_year"),
        ])
        type = type.lower()

        agg_expr = "federal_action_obligation"

        if year not in df.select(pl.col("year").unique()).to_series():
            raise ValueError(f"Year {year} not found in the DataFrame.")

        match type:
            case 'fiscal':
                df_filtered = df.filter(pl.col("pr_fiscal_year") == year)
            case 'yearly':
                df_filtered = df.filter(pl.col("year") == year)
            case 'monthly':
                df_filtered = df.filter((pl.col("month") == month) & (pl.col("year") == year))
            case 'quarterly':
                quarter_to_calendar_month = {
                    1: [1, 2, 3], 
                    2: [4, 5, 6], 
                    3: [7, 8, 9],
                    4: [10, 11, 12]
                }
                df_filtered = df.filter((pl.col("month").is_in(quarter_to_calendar_month[quarter])) & (pl.col("year") == year))
        grouped_df = df_filtered.group_by([category]).agg(pl.col(agg_expr).sum())

        return grouped_df, columns
    

    def process_energy_data(
    self,
    period: str = "monthly",  
    metric: str = "generacion_neta_mkwh"
) -> pl.DataFrame:
        self.pull_energy_data()
        self.insert_energy_data()
        df = self.conn.sql("SELECT * FROM EnergyTable").pl()

        month_map = {
            1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
            7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec',
        }
        months = list(month_map.values())
        excluded_columns = ["mes"]
        columns = [
            {"value": col, "label": col.replace("_", " ").capitalize()}
            for col in df.columns
            if col not in excluded_columns
        ]
        df = (
            df.with_columns(
                pl.col("mes").str.strptime(pl.Date, "%m/%d/%Y", strict=False).alias("date")
            )
            .with_columns([
                pl.col("date").dt.year().alias("year"),
                pl.col("date").dt.month().alias("month"),
            ])
            .with_columns(
                pl.col("month").cast(pl.String).replace(month_map).alias("month_name"),
                (pl.col("year") + (pl.col("month") > 6).cast(pl.Int32)).alias("fiscal"),
            )
        )

        agg_expr = pl.col(metric).sum().alias(metric)
        period = period.lower()

        match period:
            case "fiscal":
                grouped_df = (
                    df.with_columns(
                        pl.col("fiscal").cast(pl.String).alias("time_period")
                    )
                    .group_by("time_period")
                    .agg(agg_expr)
                )

            case "yearly":
                grouped_df = (
                    df.with_columns(
                        pl.col("year").cast(pl.String).alias("time_period")
                    )
                    .group_by("time_period")
                    .agg(agg_expr)
                )
            case "quarterly":
                quarter_expr = (
                    pl.when(pl.col("month").is_in([1, 2, 3])).then(pl.lit("q1"))
                    .when(pl.col("month").is_in([4, 5, 6])).then(pl.lit("q2"))
                    .when(pl.col("month").is_in([7, 8, 9])).then(pl.lit("q3"))
                    .otherwise(pl.lit("q4"))
                )
                grouped_df = (
                    df.with_columns(
                        (pl.col("year").cast(pl.String) + "-" + quarter_expr)
                            .alias("time_period")
                    )
                    .group_by("time_period")
                    .agg(agg_expr)
                )

            case "monthly":
                results = pl.DataFrame(schema={
                    "month_name":  pl.String,
                    metric:        pl.Float64,
                    "year":        pl.Int32,
                    "time_period": pl.String,
                })
                months_df = pl.DataFrame({"month_name": months})

                for yr in df.select("year").unique().to_series():
                    df_year = df.filter(pl.col("year") == yr)

                    df_year = months_df.join(df_year, on="month_name", how="left")
                    df_year = df_year.select("month_name", metric, "year").with_columns(
                        pl.col("year").fill_null(yr),
                        pl.col(metric).fill_null(0)
                    )
                    df_year = df_year.group_by(["month_name", "year"]).agg(agg_expr)
                    df_year = df_year.with_columns(
                        pl.col(metric).cast(pl.Float64)
                    )

                    df_year = (
                        df_year
                        .with_columns(
                            (pl.col("year").cast(pl.String) + pl.col("month_name"))
                                .alias("time_period")
                        )
                        .select("month_name", metric, "year", "time_period")
                    )

                    results = pl.concat([results, df_year])

                grouped_df = results

            case _:
                raise ValueError("period debe ser monthly | quarterly | yearly | fiscal")

        return grouped_df, columns                
      
   
    def process_price_indexes(
        self,
        time_frame: str,
        data_type: str
    ):
        if time_frame == 'yearly':
            df = pl.read_excel(f"{self.saving_dir}raw/price_indexes.xlsx", sheet_id=1)
            df = df.with_columns(
                (pl.col("year").cast(str)).alias("time_period")
            )
            df = df.drop(['year'])
        elif time_frame == 'quarterly':
            df = pl.read_excel(f"{self.saving_dir}raw/price_indexes.xlsx", sheet_id=2)
            df = df.with_columns(
                (pl.col("year").cast(str) + "-Q" + pl.col("qtr").cast(str)).alias("time_period")
            )
            df = df.drop(['year', 'qtr'])
        elif time_frame == 'fiscal':
            df = pl.read_excel(f"{self.saving_dir}raw/price_indexes.xlsx", sheet_id=3)
            df = df.with_columns(
                (pl.col("fiscal_year").cast(str)).alias("time_period")
            )
            df = df.drop(['fiscal_year'])
        elif time_frame == 'monthly':
            df = pl.read_excel(f"{self.saving_dir}raw/price_indexes.xlsx", sheet_id=4)
            df = df.with_columns(
                (pl.col("year").cast(str) + "-" + pl.col("month").cast(str).str.zfill(2)).alias("time_period")
            )
            df = df.drop(['year', 'month'])
        else:
            raise ValueError("Invalid time frame. Choose from 'yearly', 'quarterly', 'fiscal' or 'monthly'.")

        df = df.drop([
            col for col in df.columns
            if df.select(
                (pl.col(col).is_null()) |
                ((pl.col(col) == "") if df.schema[col] == pl.String else False)
            ).to_series().any()
        ])
        df = df.sort("time_period")

        if data_type == 'cambio_porcentual':
            df = df.with_columns([
                (pl.col(col).cast(pl.Float64) - 100).alias(col) for col in df.columns  if col != "time_period"
            ])
        elif data_type == 'primera_diferencia':
            df = df.with_columns([
                pl.col(col).diff(n=1).alias(col) for col in df.columns if col != "time_period"
            ])
        elif data_type == 'indices_precio':
            df = df
        else:
            raise ValueError("Invalid data type. Choose from 'cambio_porcentual', 'primera_diferencia' or 'indices_precio'.")

        df.write_csv(f"{self.saving_dir}processed/{time_frame}-{data_type}-price_indexes.csv")

        return df
