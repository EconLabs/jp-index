from tqdm import tqdm
import ibis
import logging
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import requests
import os
import logging
import ibis
import zipfile
import polars as pl


class DataPull:
    def __init__(
        self,
        saving_dir: str = "data/",
        database_url: str = "duckdb:///data.ddb",
    ):
        self.database_url = database_url
        self.saving_dir = saving_dir
        self.data_file = self.database_url.split("///")[1]
        self.conn = ibis.duckdb.connect(f"{self.data_file}")

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%d-%b-%y %H:%M:%S",
            filename="data_process.log",
        )
        # Check if the saving directory exists
        if not os.path.exists(self.saving_dir + "raw"):
            os.makedirs(self.saving_dir + "raw")
        if not os.path.exists(self.saving_dir + "processed"):
            os.makedirs(self.saving_dir + "processed")
        if not os.path.exists(self.saving_dir + "external"):
            os.makedirs(self.saving_dir + "external")

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

    def pull_economic_indicators(self, file_path: str):
        url = "https://jp.pr.gov/wp-content/uploads/2024/09/Indicadores_Economicos_9.13.2024.xlsx"
        self.pull_file(url, file_path)

    def clean_awards_by_year(self, fiscal_year: int):

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
            pl.Series("disaster_emergency_fund_codes_for_overall_award", [], dtype=pl.Utf8),
            pl.Series("outlayed_amount_from_COVID-19_supplementals_for_overall_award", [], dtype=pl.Float64), 
            pl.Series("obligated_amount_from_COVID-19_supplementals_for_overall_award", [], dtype=pl.Float64), 
            pl.Series("outlayed_amount_from_IIJA_supplemental_for_overall_award", [], dtype=pl.Float64), 
            pl.Series("obligated_amount_from_IIJA_supplemental_for_overall_award", [], dtype=pl.Float64), 
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
            pl.Series("prime_award_transaction_recipient_county_fips_code", [], dtype=pl.Utf8),
            pl.Series("recipient_county_name", [], dtype=pl.Utf8),
            pl.Series("prime_award_transaction_recipient_state_fips_code", [], dtype=pl.Utf8),
            pl.Series("recipient_state_code", [], dtype=pl.Utf8),
            pl.Series("recipient_state_name", [], dtype=pl.Utf8),
            pl.Series("recipient_zip_code", [], dtype=pl.Utf8),
            pl.Series("recipient_zip_last_4_code", [], dtype=pl.Utf8),
            pl.Series("prime_award_transaction_recipient_cd_original", [], dtype=pl.Utf8),
            pl.Series("prime_award_transaction_recipient_cd_current", [], dtype=pl.Utf8),
            pl.Series("recipient_foreign_city_name", [], dtype=pl.Utf8),
            pl.Series("recipient_foreign_province_name", [], dtype=pl.Utf8),
            pl.Series("recipient_foreign_postal_code", [], dtype=pl.Utf8),
            pl.Series("primary_place_of_performance_scope", [], dtype=pl.Utf8),
            pl.Series("primary_place_of_performance_country_code", [], dtype=pl.Utf8),
            pl.Series("primary_place_of_performance_country_name", [], dtype=pl.Utf8),
            pl.Series("primary_place_of_performance_code", [], dtype=pl.Utf8),
            pl.Series("primary_place_of_performance_city_name", [], dtype=pl.Utf8),
            pl.Series("prime_award_transaction_place_of_performance_county_fips_code", [], dtype=pl.Utf8),
            pl.Series("primary_place_of_performance_county_name", [], dtype=pl.Utf8),
            pl.Series("prime_award_transaction_place_of_performance_state_fips_code", [], dtype=pl.Utf8),
            pl.Series("primary_place_of_performance_state_name", [], dtype=pl.Utf8),
            pl.Series("primary_place_of_performance_zip_4", [], dtype=pl.Utf8),
            pl.Series("prime_award_transaction_place_of_performance_cd_original", [], dtype=pl.Utf8),
            pl.Series("prime_award_transaction_place_of_performance_cd_current", [], dtype=pl.Utf8),
            pl.Series("primary_place_of_performance_foreign_location", [], dtype=pl.Utf8),
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
            pl.Series("fiscal_year", [], dtype=pl.Int64)
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
                pl.col("outlayed_amount_from_COVID-19_supplementals_for_overall_award").cast(pl.Float64),
                pl.col("obligated_amount_from_COVID-19_supplementals_for_overall_award").cast(pl.Float64),
                pl.col("outlayed_amount_from_IIJA_supplemental_for_overall_award").cast(pl.Float64),
                pl.col("obligated_amount_from_IIJA_supplemental_for_overall_award").cast(pl.Float64),
                pl.col("action_date").str.strptime(pl.Date, format="%Y-%m-%d"),
                pl.col("action_date_fiscal_year").cast(pl.Int64),
                pl.col("period_of_performance_start_date").str.strptime(pl.Date, format="%Y-%m-%d"),
                pl.col("period_of_performance_current_end_date").str.strptime(pl.Date, format="%Y-%m-%d"),
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
                pl.col("prime_award_transaction_recipient_county_fips_code").cast(pl.Utf8),
                pl.col("recipient_county_name").cast(pl.Utf8),
                pl.col("prime_award_transaction_recipient_state_fips_code").cast(pl.Utf8),
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
                pl.col("prime_award_transaction_place_of_performance_county_fips_code").cast(pl.Utf8),
                pl.col("primary_place_of_performance_county_name").cast(pl.Utf8),
                pl.col("prime_award_transaction_place_of_performance_state_fips_code").cast(pl.Utf8),
                pl.col("primary_place_of_performance_state_name").cast(pl.Utf8),
                pl.col("primary_place_of_performance_zip_4").cast(pl.Utf8),
                pl.col("prime_award_transaction_place_of_performance_cd_original").cast(pl.Utf8),
                pl.col("prime_award_transaction_place_of_performance_cd_current").cast(pl.Utf8),
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
                pl.col("fiscal_year").cast(pl.Int64)
            ]
        )

        acs = pl.concat([acs, df], how="vertical")
        logging.info(f"Cleaned data for fiscal year {fiscal_year}.")

        acs = acs.with_columns(
            [
                pl.col("action_date").cast(pl.Utf8).fill_null(pl.lit(None)),
                pl.col("period_of_performance_start_date").cast(pl.Utf8).fill_null(pl.lit(None)),
                pl.col("period_of_performance_current_end_date").cast(pl.Utf8).fill_null(pl.lit(None)),
                pl.col("initial_report_date").cast(pl.Utf8).fill_null(pl.lit(None)),
                pl.col("last_modified_date").cast(pl.Utf8).fill_null(pl.lit(None)),
            ]
        )

        return acs

    def pull_awards_by_year(self, fiscal_year: int):
        base_url = "https://api.usaspending.gov/api/v2/bulk_download/awards/"
        headers = {"Content-Type": "application/json"}

        payload = {
                "filters": {
                    "prime_award_types": [
                        "02",
                        "03",
                        "04",
                        "05"
                    ],
                    "date_type": "action_date",
                    "date_range": {
                        "start_date": f"{fiscal_year - 1}-10-01",
                        "end_date": f"{fiscal_year}-09-30"
                    },
                     "place_of_performance_locations": [{"country": "USA", "state": "PR"}],
                },
                "subawards": False,
                "order": "desc",
                "sort": "total_obligated_amount",
                "file_format": "csv"
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
                    return None
                logging.info(f"Downloaded file for fiscal year: {fiscal_year}.")
                file_path = f"data/raw/{fiscal_year}_spending.zip"
                self.pull_file(url, file_path)
                self.extract_awards_by_year(fiscal_year)
                df = self.clean_awards_by_year(fiscal_year)
                return df

            else:
                logging.error(f"Error en la solicitud: {response.status_code}, {response.reason}")

        except Exception as e:
            logging.error(f"Error al realizar la solicitud: {e}")

    def extract_awards_by_year(self, year: int):
        extracted = False
        data_directory = "data/raw/"
        local_zip_path = os.path.join(data_directory, f"{year}_spending.zip")
            
        with zipfile.ZipFile(local_zip_path, "r") as zip_ref:
            zip_ref.extractall(data_directory)
            logging.info("Extracted file.")
            extracted = True
        extracted_files = [f for f in os.listdir(data_directory) if f.endswith('.csv')]
        if extracted:
            latest_file = max(extracted_files, key=lambda f: os.path.getmtime(os.path.join(data_directory, f)))
            new_name = f"{year}_spending.csv"
            old_path = os.path.join(data_directory, latest_file)
            new_path = os.path.join(data_directory, new_name)
            os.rename(old_path, new_path)
        else:
            logging.info("No extracted files found.")
        return None

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
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": "http://www.mercadolaboral.pr.gov",
            "DNT": "1",
            "Connection": "keep-alive",
            "Referer": "http://www.mercadolaboral.pr.gov/Tablas_Estadisticas/Otras_Tablas/T_Indice_Precio.aspx",
            "Upgrade-Insecure-Requests": "1",
            "Sec-GPC": "1",
            "Priority": "u=0, i",
        }
        data = {
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": "/wEPDwUKLTcxODY5NDM5NGQYAQUeX19Db250cm9sc1JlcXVpcmVQb3N0QmFja0tleV9fFgQFEmN0bDAwJEltYWdlQnV0dG9uMgUSY3RsMDAkSW1hZ2VCdXR0b243BRJjdGwwMCRJbWFnZUJ1dHRvbjYFE2N0bDAwJEltYWdlQnV0dG9uMjmbsvgxbeWK6OTKG+ORpFm3OEWkw9qqUvxB+x/6e4VYHA==",
            "__VIEWSTATEGENERATOR": "C7F80305",
            "__PREVIOUSPAGE": "GwKz4yZIj7Uo4a3KIzGOhMCI77vcHU60FinRHRx3g5l-ETFOoBNr2no6Fz0HhRbD6EHh7WoeeB373TP67fTBUnA29kstVUAarj4oAEiJ6fSKHGteKw7kiIEmcASoaXYmTyut_TAQm5UxmEzDwNnRMA2",
            "__EVENTVALIDATION": "/wEdAAlUqk7OF8+IyJVVhTuf5Y1+K54MsQ9Z5Tipa4C3CU9lIy6KqsTtzWiK229TcIgvoTmJ5D8KsXArXsSdeMqOt6pk+d3fBy3LDDz0lsNt4u+CuDIENRTx3TqpeEC0BFNcbx18XLv2PDpbcvrQF1sPng9RHC+hNwNMKsAjTYpq3ZLON4FBZYDVNXrnB/9WmjDFKj6ji8qalfcp0F7IzcRWfkdgwm54EtTOkeRtMO19pSuuIg==",
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
