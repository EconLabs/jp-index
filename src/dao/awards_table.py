from typing import Optional
from sqlmodel import Field, SQLModel

class AwardTable(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    award_id: str
    recipient_name: Optional[str]
    start_date: Optional[str]
    end_date: Optional[str]
    award_amount: Optional[float]
    awarding_agency: Optional[str]
    awarding_sub_agency: Optional[str]
    funding_agency: Optional[str]
    funding_sub_agency: Optional[str]
    award_type: Optional[str]

def create_awards_table(engine):
    AwardTable.metadata.create_all(engine)
