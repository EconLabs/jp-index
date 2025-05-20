from tqdm import tqdm
import logging
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import requests
import os
import zipfile
import polars as pl
from ..models import (
    get_conn,
    init_activity_table,
    init_awards_table,
    init_consumer_table,
)


class DataPull:
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
        Parameters
        ----------
        update : bool
            Whether to update the data. Defaults to False.

        Returns
        -------
        pl.DataFrame
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

    def pull_economic_indicators(self, file_path: str) -> None:
        url = "https://jp.pr.gov/wp-content/uploads/2024/09/Indicadores_Economicos_9.13.2024.xlsx"
        self.pull_file(url, file_path)

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
                self.pull_file(url, file_path)
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
            ).df()
            if result.empty:
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

    def pull_consumer(self, file_path: str):
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
        url = "https://www.bde.pr.gov/BDE/PREDDOCS/I_EAI.XLS"
        self.pull_file(url, file_path)
        logging.info(f"Downloaded file to {file_path}")

    def pull_file(self, url: str, filename: str, verify: bool = True) -> None:
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
