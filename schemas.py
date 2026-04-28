from pydantic import BaseModel, Field
from typing import List, Optional

class WeddingInfo(BaseModel):
    couple_names: Optional[str] = Field(None, description="Names of the couple getting married")
    date: Optional[str] = Field(None, description="Date of the wedding")
    time: Optional[str] = Field(None, description="Time of the wedding")
    venue: Optional[str] = Field(None, description="Name and address of the wedding venue")
    location: Optional[str] = Field(None, description="City or region where the wedding takes place")
    source_url: str = Field(..., description="URL where the information was found")

class WeddingList(BaseModel):
    weddings: List[WeddingInfo]
