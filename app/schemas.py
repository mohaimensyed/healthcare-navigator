from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any
import re

class ProviderResponse(BaseModel):
    """Response model for provider search results"""
    provider_id: str = Field(..., description="CMS provider identifier")
    provider_name: str = Field(..., description="Hospital name")
    provider_city: str = Field(..., description="Hospital city")
    provider_state: str = Field(..., description="Hospital state")
    provider_zip_code: str = Field(..., description="Hospital ZIP code")
    ms_drg_definition: str = Field(..., description="MS-DRG procedure description")
    total_discharges: int = Field(..., ge=0, description="Number of procedures performed")
    average_covered_charges: float = Field(..., ge=0, description="Average hospital charges in dollars")
    average_total_payments: float = Field(..., ge=0, description="Average total payments in dollars")
    average_medicare_payments: float = Field(..., ge=0, description="Average Medicare payments in dollars")
    average_rating: Optional[float] = Field(None, ge=1.0, le=10.0, description="Average quality rating (1-10 scale)")
    distance_km: Optional[float] = Field(None, ge=0, description="Distance from search location in kilometers")
    
    @validator('average_rating')
    def round_rating(cls, v):
        """Round rating to 1 decimal place"""
        if v is not None:
            return round(v, 1)
        return v
    
    @validator('distance_km')
    def round_distance(cls, v):
        """Round distance to 2 decimal places"""
        if v is not None:
            return round(v, 2)
        return v
    
    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "provider_id": "330123",
                "provider_name": "MOUNT SINAI HOSPITAL",
                "provider_city": "NEW YORK",
                "provider_state": "NY",
                "provider_zip_code": "10029",
                "ms_drg_definition": "470 - Major Joint Replacement w/o MCC",
                "total_discharges": 245,
                "average_covered_charges": 84621.50,
                "average_total_payments": 21515.75,
                "average_medicare_payments": 19024.25,
                "average_rating": 8.5,
                "distance_km": 12.34
            }
        }

class ProviderSearchParams(BaseModel):
    """Request parameters for provider search"""
    drg: str = Field(..., description="MS-DRG code or description", min_length=1, max_length=200)
    zip_code: str = Field(..., description="ZIP code for search center", min_length=5, max_length=10)
    radius_km: int = Field(default=50, description="Search radius in kilometers", ge=1, le=500)
    limit: int = Field(default=50, description="Maximum number of results", ge=1, le=100)
    
    @validator('zip_code')
    def validate_zip_code(cls, v):
        """Validate ZIP code format"""
        if not v or not v.strip():
            raise ValueError("ZIP code cannot be empty")
        
        # Remove any whitespace
        zip_code = v.strip()
        
        # Check for basic ZIP code format (5 digits, optionally followed by -4 digits)
        if not re.match(r'^\d{5}(-\d{4})?$', zip_code):
            raise ValueError("ZIP code must be 5 digits, optionally followed by -4 digits")
        
        return zip_code
    
    @validator('drg')
    def validate_drg(cls, v):
        """Validate DRG input"""
        if not v or not v.strip():
            raise ValueError("DRG cannot be empty")
        
        drg = v.strip()
        
        # Check length
        if len(drg) > 200:
            raise ValueError("DRG description too long (max 200 characters)")
        
        return drg

class AskRequest(BaseModel):
    """Request model for AI assistant questions"""
    question: str = Field(..., description="Natural language question about healthcare providers", min_length=5, max_length=1000)
    
    @validator('question')
    def validate_question(cls, v):
        """Validate question input"""
        if not v or not v.strip():
            raise ValueError("Question cannot be empty")
        
        question = v.strip()
        
        # Check for minimum meaningful length
        if len(question) < 5:
            raise ValueError("Question too short (minimum 5 characters)")
        
        # Check for maximum length to prevent abuse
        if len(question) > 1000:
            raise ValueError("Question too long (maximum 1000 characters)")
        
        return question
    
    class Config:
        json_schema_extra = {
            "example": {
                "question": "Who is the cheapest for knee replacement near 10001?"
            }
        }

class AskResponse(BaseModel):
    """Response model for AI assistant answers"""
    answer: str = Field(..., description="Natural language answer to the question")
    sql_query: Optional[str] = Field(None, description="SQL query used to retrieve data (for debugging)")
    data_used: Optional[List[Dict[str, Any]]] = Field(None, description="Sample of data used to generate answer")
    
    class Config:
        json_schema_extra = {
            "example": {
                "answer": "Based on the data, Mount Sinai Hospital offers the lowest cost for knee replacement near 10001, with charges of $45,230. They also have a good rating of 8.2/10.",
                "sql_query": "SELECT provider_name, average_covered_charges FROM providers WHERE ms_drg_definition ILIKE '%knee%' ORDER BY average_covered_charges ASC LIMIT 5;",
                "data_used": [
                    {
                        "provider_name": "MOUNT SINAI HOSPITAL",
                        "average_covered_charges": 45230.00,
                        "average_rating": 8.2
                    }
                ]
            }
        }

class HealthCheckResponse(BaseModel):
    """Response model for health check endpoint"""
    status: str = Field(..., description="Service health status")
    service: str = Field(..., description="Service name")
    database: str = Field(..., description="Database connection status")
    providers_in_db: int = Field(..., description="Number of providers in database")
    version: str = Field(..., description="API version")

class StatisticsResponse(BaseModel):
    """Response model for statistics endpoint"""
    total_providers: int = Field(..., description="Total number of unique providers")
    total_drgs: int = Field(..., description="Total number of unique DRG procedures")
    average_cost: float = Field(..., description="Average cost across all procedures")
    total_ratings: int = Field(..., description="Total number of ratings")
    average_rating: float = Field(..., description="Average rating across all providers")

class ExamplesResponse(BaseModel):
    """Response model for example prompts endpoint"""
    examples: List[str] = Field(..., description="List of example questions for the AI assistant")
    description: str = Field(..., description="Description of the examples")

class ErrorResponse(BaseModel):
    """Standard error response model"""
    detail: str = Field(..., description="Error message")
    error_code: Optional[str] = Field(None, description="Optional error code")
    timestamp: Optional[str] = Field(None, description="Error timestamp")
    
    class Config:
        json_schema_extra = {
            "example": {
                "detail": "Invalid ZIP code format",
                "error_code": "VALIDATION_ERROR",
                "timestamp": "2024-01-15T10:30:00Z"
            }
        }

class RatingResponse(BaseModel):
    """Response model for individual ratings"""
    id: int = Field(..., description="Rating ID")
    provider_id: str = Field(..., description="Provider ID")
    rating: float = Field(..., ge=1.0, le=10.0, description="Rating value (1-10)")
    category: str = Field(..., description="Rating category")
    
    class Config:
        from_attributes = True

class ProviderDetailResponse(ProviderResponse):
    """Extended provider response with ratings details"""
    ratings: List[RatingResponse] = Field(default_factory=list, description="Individual ratings for this provider")
    
    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                **ProviderResponse.Config.json_schema_extra["example"],
                "ratings": [
                    {
                        "id": 1,
                        "provider_id": "330123",
                        "rating": 8.5,
                        "category": "overall"
                    },
                    {
                        "id": 2,
                        "provider_id": "330123", 
                        "rating": 9.0,
                        "category": "cardiac"
                    }
                ]
            }
        }

# Input validation utilities
class ValidationUtils:
    """Utility functions for input validation"""
    
    @staticmethod
    def is_valid_zip_code(zip_code: str) -> bool:
        """Check if ZIP code format is valid"""
        if not zip_code:
            return False
        return bool(re.match(r'^\d{5}(-\d{4})?$', zip_code.strip()))
    
    @staticmethod
    def is_valid_drg_code(drg: str) -> bool:
        """Check if DRG code format is valid"""
        if not drg:
            return False
        # DRG codes are typically 3 digits
        return bool(re.match(r'^\d{3}$', drg.strip()))
    
    @staticmethod
    def clean_zip_code(zip_code: str) -> str:
        """Clean and standardize ZIP code format"""
        if not zip_code:
            return ""
        # Return just the 5-digit portion
        return zip_code.strip().split('-')[0]
    
    @staticmethod
    def format_currency(amount: float) -> str:
        """Format amount as currency"""
        return f"${amount:,.2f}"
    
    @staticmethod
    def format_rating(rating: float) -> str:
        """Format rating with /10 suffix"""
        return f"{rating:.1f}/10"