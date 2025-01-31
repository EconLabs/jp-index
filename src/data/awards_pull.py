import time

import requests


class AwardsPuller:
    def __init__(self):
        self.base_url = "https://api.usaspending.gov/api/v2/bulk_download/awards/"
        self.headers = {"Content-Type": "application/json"}
        self.retry_count = 3

    def _make_api_request(self, payload):
        try:
            response = requests.post(
                self.base_url, json=payload, headers=self.headers, timeout=120
            )
            if response.status_code == 200:
                response_json = response.json()
                return response_json.get("status_url")
            else:
                print(f"Error en la solicitud: {response.status_code}, {response.text}")
                return None
        except Exception as e:
            print(f"Error al realizar la solicitud: {e}")
            return None

    def check_status(self, status_url):
        print(f"Verificando el estado en: {status_url}")
        for attempt in range(5):
            try:
                response = requests.get(status_url, headers=self.headers, timeout=60)
                if response.status_code == 200:
                    status_data = response.json()
                    print(f"Estado: {status_data.get('status')}")
                    if status_data.get("status") == "finished":
                        print(
                            f"Archivo listo para descargar: {status_data.get('file_url')}"
                        )
                        return status_data.get("file_url")
                    elif status_data.get("status") == "failed":
                        print(f"Error en la generación del archivo: {status_data}")
                        return None
                    else:
                        print(
                            f"Estado actual: {status_data.get('status')}. Esperando..."
                        )
                        time.sleep(10)
                else:
                    print(
                        f"Error al verificar el estado: {response.status_code}, {response.text}"
                    )
            except Exception as e:
                print(f"Error al verificar el estado: {e}")
        print("Se agotaron los intentos para verificar el estado.")
        return None

    def download_awards_data(self, payload):
        status_url = self._make_api_request(payload)
        if not status_url:
            print(
                "No se pudo obtener el status_url. Revisa el payload y la conectividad."
            )
            return

        file_url = self.check_status(status_url)
        if file_url:
            print(f"Archivo generado: {file_url}")
        else:
            print("No se pudo generar el archivo.")


puller = AwardsPuller()

payload = {
    "filters": {
        "prime_award_types": ["A", "B", "C", "D"],
        "date_type": "action_date",
        "date_range": {
            "start_date": "2019-10-01",
            "end_date": "2020-09-30",
        },
        "place_of_performance_locations": [{"country": "USA", "state": "PR"}],
    },
    "file_format": "csv",
}

puller.download_awards_data(payload)
