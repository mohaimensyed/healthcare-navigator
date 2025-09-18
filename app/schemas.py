from pydantic import BaseModel, Field
from typing import List, Optional

class ProviderResponse(BaseModel):
    provider_id: str
    provider_name: str
    provider_city: str
    provider_state: str
    provider_zip_code: str
    ms_drg_definition: str
    total_discharges: int
    average_covered_charges: float
    average_total_payments: float
    average_medicare_payments: float
    average_rating: Optional[float] = None
    distance_km: Optional[float] = None
    
    class Config:
        from_attributes = True

class ProviderSearchParams(BaseModel):
    drg: str = Field(..., description="MS-DRG code or description")
    zip_code: str = Field(..., description="ZIP code for search center")
    radius_km: int = Field(default=50, description="Search radius in kilometers")
    limit: int = Field(default=50, description="Maximum number of results")

class AskRequest(BaseModel):
    question: str = Field(..., description="Natural language question about healthcare providers")

class AskResponse(BaseModel):
    answer: str
    sql_query: Optional[str] = None
    data_used: Optional[List[dict]] = None