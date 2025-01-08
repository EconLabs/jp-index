from typing import Optional
from sqlmodel import Field, SQLModel

class AwardTable(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)  # Auto-increment ID
    internal_id: Optional[int]  # ID interno de la API
    award_id: Optional[str] = Field(alias="Award ID")  # Prime Award ID
    recipient_name: Optional[str] = Field(alias="Recipient Name")  # Nombre del receptor
    start_date: Optional[str] = Field(alias="Start Date")  # Fecha de inicio del periodo
    end_date: Optional[str] = Field(alias="End Date")  # Fecha de fin del periodo
    award_amount: Optional[float] = Field(alias="Award Amount")  # Cantidad del premio
    awarding_agency: Optional[str] = Field(alias="Awarding Agency")  # Agencia otorgante
    awarding_subagency: Optional[str] = Field(alias="Awarding Sub Agency")  # Sub-agencia otorgante
    funding_agency: Optional[str] = Field(alias="Funding Agency")  # Agencia financiadora
    funding_subagency: Optional[str] = Field(alias="Funding Sub Agency")  # Sub-agencia financiadora
    award_type: Optional[str] = Field(alias="Award Type")  # Tipo de premio
    awarding_agency_id: Optional[int] = Field(alias="awarding_agency_id")  # ID de la agencia otorgante
    agency_slug: Optional[str] = Field(alias="agency_slug")  # Identificador único de la agencia
    generated_internal_id: Optional[str] = Field(alias="generated_internal_id")  # ID interno generado



def create_award_table(engine):
    """
    Crea la tabla 'awardtable' si no existe.
    """
    SQLModel.metadata.create_all(engine)
