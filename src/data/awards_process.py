from datetime import datetime, timedelta
import pandas as pd
from sqlmodel import create_engine, SQLModel
from src.dao.awards_table import create_award_table
from src.data.pull_awards import (
    get_data_for_month,
)


DATABASE_URL = "sqlite:///db.sqlite"


class AwardDataProcessor:
    def __init__(self, database_url: str = DATABASE_URL):
        self.engine = create_engine(database_url)
        create_award_table(self.engine)

    def clean_data(self, raw_data: list) -> pd.DataFrame:
        """
        Limpia y transforma los datos para la base de datos.
        """
        df = pd.DataFrame(raw_data)
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

        # Renombrar columnas según el mapeo
        df = df.rename(columns=column_mapping)

        # Asegurar que todas las columnas existan
        for col in column_mapping.values():
            if col not in df.columns:
                df[col] = None

        # Convertir tipos de datos
        df["award_amount"] = pd.to_numeric(df["award_amount"], errors="coerce").fillna(
            0
        )
        df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce").dt.date
        df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce").dt.date

        return df

    def process_and_insert(self, start_date: str, end_date: str):
        """
        Procesa e inserta los datos en la base de datos.
        """
        raw_data = get_data_for_month(start_date, end_date)
        if not raw_data:
            print(f"No se descargaron datos para el rango {start_date} - {end_date}.")
            return

        cleaned_data = self.clean_data(raw_data)
        print(
            f"Datos limpiados para el rango {start_date} - {end_date} (vista previa):"
        )
        print(cleaned_data.head())

        try:
            with self.engine.begin() as connection:
                cleaned_data.to_sql(
                    "awardtable", con=connection, if_exists="append", index=False
                )
            print(
                f"Datos insertados exitosamente para el rango {start_date} - {end_date}."
            )
        except Exception as e:
            print(
                f"Error insertando datos para el rango {start_date} - {end_date}: {e}"
            )


#if __name__ == "__main__":
def call_test():
    processor = AwardDataProcessor()

    start_year = 2013
    end_year = 2013

    for year in range(start_year, end_year + 1):
        for month in range(1, 3):
            month_start = datetime(year, month, 1).strftime("%Y-%m-%d")
            next_month = (datetime(year, month, 28) + timedelta(days=4)).replace(day=1)
            month_end = (next_month - timedelta(days=1)).strftime("%Y-%m-%d")

            print(f"Procesando datos para el rango {month_start} - {month_end}...")
            processor.process_and_insert(month_start, month_end)
            #print(len(get_data_for_month(month_start, month_end)))
            #print(get_data_for_month(month_start, month_end, 100))
