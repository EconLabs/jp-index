import sqlite3

def get_conn(db_path: str) -> sqlite3.Connection:
    return sqlite3.connect(db_path)

def init_award_data_table(db_path: str) -> None:
    conn = get_conn(db_path)
    cursor = conn.cursor()
    
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS AwardTable (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            internal_id INTEGER,
            award_id TEXT,
            recipient_name TEXT,
            start_date TEXT,
            end_date TEXT,
            award_amount FLOAT,
            awarding_agency TEXT,
            awarding_subagency TEXT,
            funding_agency TEXT,
            funding_subagency TEXT,
            award_type TEXT,
            awarding_agency_id INTEGER,
            agency_slug TEXT,
            generated_internal_id TEXT,
            page INTEGER,
            fiscal_year INTEGER
        )
        """
    )
    conn.commit()
    conn.close()