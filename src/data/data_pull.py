from tqdm import tqdm
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import requests
import os
import logging
import ibis
import zipfile
import polars as pl
import time


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

    def clean_awards_by_year(self, data: list, page: int, fiscal_year: int):
        empty_df = [
            pl.Series("internal_id", [], dtype=pl.Int64),
            pl.Series("Award ID", [], dtype=pl.String),
            pl.Series("Recipient Name", [], dtype=pl.String),
            pl.Series("Start Date", [], dtype=pl.Date),
            pl.Series("End Date", [], dtype=pl.Date),
            pl.Series("Award Amount", [], dtype=pl.Float64),
            pl.Series("Awarding Agency", [], dtype=pl.String),
            pl.Series("Awarding Sub Agency", [], dtype=pl.String),
            pl.Series("Funding Agency", [], dtype=pl.String),
            pl.Series("Funding Sub Agency", [], dtype=pl.String),
            pl.Series("Award Type", [], dtype=pl.String),
            pl.Series("awarding_agency_id", [], dtype=pl.Int64),
            pl.Series("agency_slug", [], dtype=pl.String),
            pl.Series("generated_internal_id", [], dtype=pl.String),
            pl.Series("page", [], dtype=pl.Int64),
        ]
        acs = pl.DataFrame(empty_df).clear()
        df = pl.DataFrame(data)
        df = df.with_columns(
            pl.lit(page).alias("page").cast(pl.Int64),
            pl.lit(fiscal_year).alias("fiscal_year").cast(pl.Int64),
        )
        df = df.with_columns(
            [
                pl.col("Start Date").str.strptime(pl.Date, format="%Y-%m-%d"),
                pl.col("End Date").str.strptime(pl.Date, format="%Y-%m-%d"),
            ]
        )

        acs = pl.concat([acs, df], how="vertical")
        logging.info(f"Cleaned data for page {page} fiscal year {fiscal_year}.")

        column_mapping = {
            "internal_id": "internal_id",
            "Award ID": "award_id",
            "Recipient Name": "recipient_name",
            "Start Date": "start_date",
            "End Date": "end_date",
            "Award Amount": "award_amount",
            "Awarding Agency": "awarding_agency",
            "Awarding Sub Agency": "awarding_subagency",
            "Funding Agency": "funding_agency",
            "Funding Sub Agency": "funding_subagency",
            "Award Type": "award_type",
            "awarding_agency_id": "awarding_agency_id",
            "agency_slug": "agency_slug",
            "generated_internal_id": "generated_internal_id",
        }

        acs = acs.rename(column_mapping)

        acs = acs.with_columns(
            [
                pl.col("start_date").cast(pl.Utf8).fill_null(pl.lit(None)),
                pl.col("end_date").cast(pl.Utf8).fill_null(pl.lit(None)),
            ]
        )

        return acs

    def pull_awards_by_year(self, year_start: int, year_end: int, page: int):
        base_url = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
        headers = {"Content-Type": "application/json"}

        payload = {
            "subawards": False,
            "limit": 100,
            "page": page,
            "filters": {
                "award_type_codes": ["A", "B", "C", "D"],
                "time_period": [
                    {
                        "start_date": f"{year_start}-10-01",
                        "end_date": f"{year_end}-09-30",
                    }
                ],
                "place_of_performance_locations": [{"country": "USA", "state": "PR"}],
            },
            "fields": [
                "Award ID",
                "Recipient Name",
                "Start Date",
                "End Date",
                "Award Amount",
                "Awarding Agency",
                "Awarding Sub Agency",
                "Funding Agency",
                "Funding Sub Agency",
                "Award Type",
            ],
        }

        retries = 5

        try:
            for attempt in range(retries):
                try:
                    logging.info(f"Downloading page: {payload['page']}")
                    response = requests.post(
                        base_url, json=payload, headers=headers, timeout=None
                    )

                    if response.status_code == 200:
                        response_json = response.json()
                        data = response_json.get("results", [])
                        if not data:
                            return None
                        logging.info(
                            f"Downloaded page: {page} for fiscal year: {fiscal_year}."
                        )
                        df = self.clean_awards_by_year(data, page, fiscal_year)
                        return df

                    else:
                        logging.error(
                            f"Error en la solicitud: {response.status_code}, {response.reason}"
                        )
                except requests.exceptions.RequestException as error:
                    wait_time = 2**attempt
                    logging.error(f"Error: {error}. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)

        except Exception as e:
            logging.error(f"Error al realizar la solicitud: {e}")

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
