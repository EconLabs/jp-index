from tqdm import tqdm
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import requests
import os
import logging
import zipfile
import pandas as pd


class DataPull:
    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%d-%b-%y %H:%M:%S",
            filename="data_process.log",
        )

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

    def pull_awards_by_year(self, year: int):
        base_url = "https://api.usaspending.gov/api/v2/bulk_download/awards/"
        headers = {"Content-Type": "application/json"}

        payload = {
            "subawards": False,
            "filters": {
                "prime_award_types": ["A", "B", "C", "D"],
                "date_type": "action_date",
                "date_range": {
                    "start_date": f"{year}-10-01",
                    "end_date": f"{year + 1}-09-30",
                },
                "place_of_performance_locations": [{"country": "USA", "state": "PR"}],
            },
            "file_format": "csv",
        }
        try:
            response = requests.post(
                base_url, json=payload, headers=headers, timeout=None
            )
            if response.status_code == 200:
                response_json = response.json()
                url = response_json.get("file_url")
            else:
                print(f"Error en la solicitud: {response.status_code}, {response.text}")
                return None

        except Exception as e:
            print(f"Error al realizar la solicitud: {e}")
            return None

        file_path = f"{self.data_dir}/{year}_spending.zip"
        print(url)
        self.pull_file(url, file_path)

    def extract_awards_by_year(self, year: int):
        extracted = False
        local_zip_path = os.path.join(self.data_dir, f"{year}_spending.zip")

        with zipfile.ZipFile(local_zip_path, "r") as zip_ref:
            zip_ref.extractall(self.data_dir)
            print("Archivo extraído correctamente.")
            logging.info("successfuly extracted files")
            extracted = True

        extracted_files = [f for f in os.listdir(self.data_dir) if f.endswith(".csv")]

        if extracted:
            latest_file = max(
                extracted_files,
                key=lambda f: os.path.getmtime(os.path.join(self.data_dir, f)),
            )

            new_name = f"{year}_spending.csv"

            old_path = os.path.join(self.data_dir, latest_file)
            new_path = os.path.join(self.data_dir, new_name)

            os.rename(old_path, new_path)
        else:
            print("No se encontraron archivos extraídos.")
            logging.error("Could not find files form extracted ")

        return None

    def clean_awards_by_year(self, year: int):
        data_directory = f"data/raw/{year}_spending.csv"

        df = pd.read_csv(data_directory)

        date_format = "%Y-%m-%d"
        for col in df.columns:
            if "date" in col.lower():
                df[col] = pd.to_datetime(df[col], errors="coerce", format=date_format)
                df[col] = df[col].dt.strftime(date_format)

        column_mapping = {
            "award_id_piid": "Prime Award ID",
            "recipient_name": "Recipient Name",
            "total_dollars_obligated": "Obligations",
            "award_type": "Award Type",
            "prime_award_base_transaction_description": "Award Description",
            "total_outlayed_amount_for_overall_award": "Outlays",
            "disaster_emergency_fund_codes_for_overall_award": "Disaster Emergency Fund Codes (DEFCs)",
            "obligated_amount_from_COVID-19_supplementals_for_overall_award": "COVID-19 Obligations",
            "outlayed_amount_from_COVID-19_supplementals_for_overall_award": "COVID-19 Outlays",
            "outlayed_amount_from_IIJA_supplemental_for_overall_award": "Infrastructure Obligations",
            "obligated_amount_from_IIJA_supplemental_for_overall_award": "Infrastructure Outlays",
            "awarding_agency_name": "Awarding Agency",
            "awarding_sub_agency_name": "Awarding Subagency",
            "period_of_performance_start_date": "Period of Performance Start",
            "period_of_performance_current_end_date": "Period of Performance End",
        }

        df.rename(columns=column_mapping, inplace=True)

        selected_columns = list(column_mapping.values())
        df = df[selected_columns]

        print(df)

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
