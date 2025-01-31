from typing import Optional
from sqlmodel import Field, SQLModel

class AwardTable(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    date: 

def create_awards_table(engine):
    AwardTable.metadata.create_all(engine)
