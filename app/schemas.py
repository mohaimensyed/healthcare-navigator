from pydantic import BaseModel, Field, validator, root_validator
from typing import List, Optional, Dict, Any, Union
import re
from datetime import datetime

class ProviderResponse(BaseModel):
    """Enhanced response model for provider search results with ranking transparency"""
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
    
    # Enhanced fields for ranking transparency
    value_score: Optional[float] = Field(None, ge=0, description="Composite value score (higher is better)")
    cost_rank: Optional[int] = Field(None, ge=1, description="Rank by cost alone (1 = cheapest)")
    rating_rank: Optional[int] = Field(None, ge=1, description="Rank by rating alone (1 = highest rated)")
    
    @validator('average_rating')
    def round_rating(cls, v):
        """Round rating to 1 decimal place"""
        if v is not None:
            return round(float(v), 1)
        return v
    
    @validator('distance_km')
    def round_distance(cls, v):
        """Round distance to 2 decimal places"""
        if v is not None:
            return round(float(v), 2)
        return v
    
    @validator('value_score')
    def round_value_score(cls, v):
        """Round value score for display"""
        if v is not None:
            return round(float(v), 1)
        return v
    
    @validator('average_covered_charges', 'average_total_payments', 'average_medicare_payments')
    def round_monetary_values(cls, v):
        """Round monetary values to 2 decimal places"""
        return round(float(v), 2)
    
    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "provider_id": "330123",
                "provider_name": "MOUNT SINAI HOSPITAL",
                "provider_city": "NEW YORK",
                "provider_state": "NY",
                "provider_zip_code": "10029",
                "ms_drg_definition": "470 - MAJOR HIP AND KNEE JOINT REPLACEMENT OR REATTACHMENT OF LOWER EXTREMITY WITHOUT MCC",
                "total_discharges": 245,
                "average_covered_charges": 84621.50,
                "average_total_payments": 21515.75,
                "average_medicare_payments": 19024.25,
                "average_rating": 8.5,
                "distance_km": 12.34,
                "value_score": 87.3,
                "cost_rank": 3,
                "rating_rank": 2
            }
        }

class ProviderSearchParams(BaseModel):
    """Enhanced request parameters for provider search with validation"""
    drg: str = Field(..., description="MS-DRG code or description", min_length=1, max_length=200)
    zip_code: str = Field(..., description="ZIP code for search center", min_length=5, max_length=10)
    radius_km: int = Field(default=50, description="Search radius in kilometers", ge=1, le=500)
    limit: int = Field(default=50, description="Maximum number of results", ge=1, le=100)
    ranking_mode: Optional[str] = Field(
        default="value", 
        description="Ranking mode: 'cost', 'rating', 'distance', or 'value' (composite)",
        pattern="^(cost|rating|distance|value)$"
    )
    
    @validator('zip_code')
    def validate_zip_code(cls, v):
        """Enhanced ZIP code validation"""
        if not v or not v.strip():
            raise ValueError("ZIP code cannot be empty")
        
        zip_code = v.strip()
        
        # Check for basic ZIP code format (5 digits, optionally followed by -4 digits)
        if not re.match(r'^\d{5}(-\d{4})?$', zip_code):
            raise ValueError("ZIP code must be 5 digits, optionally followed by -4 digits (e.g., 10001 or 10001-1234)")
        
        # Validate that it's a reasonable US ZIP code range
        zip_num = int(zip_code[:5])
        if zip_num < 1 or zip_num > 99999:
            raise ValueError("ZIP code must be between 00001 and 99999")
        
        return zip_code
    
    @validator('drg')
    def validate_drg(cls, v):
        """Enhanced DRG validation"""
        if not v or not v.strip():
            raise ValueError("DRG cannot be empty")
        
        drg = v.strip()
        
        if len(drg) > 200:
            raise ValueError("DRG description too long (max 200 characters)")
        
        # Check for potentially malicious input
        dangerous_patterns = [
            r'<script', r'javascript:', r'vbscript:', r'onload=', r'onerror=',
            r'drop\s+table', r'delete\s+from', r'insert\s+into', r'update\s+.*set'
        ]
        
        drg_lower = drg.lower()
        for pattern in dangerous_patterns:
            if re.search(pattern, drg_lower):
                raise ValueError("Invalid characters detected in DRG input")
        
        return drg
    
    @validator('radius_km')
    def validate_radius(cls, v):
        """Validate search radius"""
        if v < 1:
            raise ValueError("Search radius must be at least 1 km")
        if v > 500:
            raise ValueError("Search radius cannot exceed 500 km")
        return v

class AskRequest(BaseModel):
    """Enhanced request model for AI assistant questions"""
    question: str = Field(..., description="Natural language question about healthcare providers", min_length=5, max_length=1000)
    context: Optional[Dict[str, Any]] = Field(None, description="Optional context for the query")
    include_debug: bool = Field(False, description="Include SQL query and data in response")
    
    @validator('question')
    def validate_question(cls, v):
        """Enhanced question validation with security checks"""
        if not v or not v.strip():
            raise ValueError("Question cannot be empty")
        
        question = v.strip()
        
        if len(question) < 5:
            raise ValueError("Question too short (minimum 5 characters)")
        
        if len(question) > 1000:
            raise ValueError("Question too long (maximum 1000 characters)")
        
        # Security check for potential injection attempts
        dangerous_patterns = [
            r'<script', r'javascript:', r'vbscript:', r'onload=', r'onerror=',
            r'drop\s+table', r'delete\s+from', r'insert\s+into', r'update\s+.*set',
            r'--\s', r'/\*.*\*/', r'xp_cmdshell', r'sp_executesql'
        ]
        
        question_lower = question.lower()
        for pattern in dangerous_patterns:
            if re.search(pattern, question_lower):
                raise ValueError("Invalid characters detected in question")
        
        return question
    
    class Config:
        json_schema_extra = {
            "example": {
                "question": "What are the best value hospitals for knee replacement near 10001?",
                "context": {"user_preferences": "balance cost and quality"},
                "include_debug": False
            }
        }

class AskResponse(BaseModel):
    """Enhanced response model for AI assistant answers"""
    answer: str = Field(..., description="Natural language answer to the question")
    intent: Optional[str] = Field(None, description="Detected query intent (cheapest, best_rated, nearest, value)")
    confidence: Optional[float] = Field(None, ge=0.0, le=1.0, description="Confidence in the answer (0-1)")
    sql_query: Optional[str] = Field(None, description="SQL query used to retrieve data (for debugging)")
    data_used: Optional[List[Dict[str, Any]]] = Field(None, description="Sample of data used to generate answer")
    ranking_explanation: Optional[str] = Field(None, description="Explanation of how results were ranked")
    
    @validator('confidence')
    def round_confidence(cls, v):
        """Round confidence to 2 decimal places"""
        if v is not None:
            return round(float(v), 2)
        return v
    
    class Config:
        json_schema_extra = {
            "example": {
                "answer": "Based on our value ranking (balancing cost, quality, distance, and experience), Mount Sinai Hospital offers the best combination for knee replacement near 10001. They charge $45,230 with an 8.2/10 rating and perform 245 procedures yearly, giving them a value score of 87.3.",
                "intent": "value",
                "confidence": 0.92,
                "sql_query": "SELECT p.provider_name, p.average_covered_charges, AVG(r.rating) FROM providers p...",
                "data_used": [
                    {
                        "provider_name": "MOUNT SINAI HOSPITAL",
                        "average_covered_charges": 45230.00,
                        "average_rating": 8.2,
                        "value_score": 87.3
                    }
                ],
                "ranking_explanation": "Results ranked by composite value score: 40% cost effectiveness, 35% quality rating, 15% distance preference, 10% volume experience"
            }
        }

class HealthCheckResponse(BaseModel):
    """Enhanced health check response with system status"""
    status: str = Field(..., description="Service health status")
    database: str = Field(..., description="Database connection status")
    providers_in_db: int = Field(..., description="Number of providers in database")
    total_ratings: int = Field(..., description="Number of ratings in database")
    average_rating: float = Field(..., description="Average rating across all providers")
    ranking_algorithm: str = Field(..., description="Current ranking algorithm description")
    version: str = Field(..., description="API version")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Health check timestamp")
    
    class Config:
        json_schema_extra = {
            "example": {
                "status": "healthy",
                "database": "connected",
                "providers_in_db": 15420,
                "total_ratings": 46260,
                "average_rating": 6.8,
                "ranking_algorithm": "composite (cost 40% + rating 35% + distance 15% + volume 10%)",
                "version": "1.0.0",
                "timestamp": "2024-01-15T10:30:00Z"
            }
        }

class StatisticsResponse(BaseModel):
    """Enhanced statistics response with ranking insights"""
    total_providers: int = Field(..., description="Total number of unique providers")
    unique_provider_ids: int = Field(..., description="Number of unique provider IDs")
    total_drgs: int = Field(..., description="Total number of unique DRG procedures")
    total_ratings: int = Field(..., description="Total number of ratings")
    
    # Cost statistics
    average_cost: float = Field(..., description="Average cost across all procedures")
    min_cost: float = Field(..., description="Minimum cost found")
    max_cost: float = Field(..., description="Maximum cost found")
    cost_std_dev: Optional[float] = Field(None, description="Standard deviation of costs")
    
    # Rating statistics
    average_rating: float = Field(..., description="Average rating across all providers")
    min_rating: float = Field(..., description="Minimum rating")
    max_rating: float = Field(..., description="Maximum rating")
    rating_std_dev: Optional[float] = Field(None, description="Standard deviation of ratings")
    
    # Ranking algorithm info
    ranking_algorithm: Dict[str, Any] = Field(..., description="Current ranking algorithm details")
    search_features: Dict[str, str] = Field(..., description="Available search features")
    
    # Geographic coverage
    states_covered: List[str] = Field(..., description="States with provider data")
    zip_code_coverage: int = Field(..., description="Number of unique ZIP codes covered")
    
    class Config:
        json_schema_extra = {
            "example": {
                "total_providers": 15420,
                "unique_provider_ids": 1240,
                "total_drgs": 145,
                "total_ratings": 46260,
                "average_cost": 25847.32,
                "min_cost": 1250.00,
                "max_cost": 485920.00,
                "cost_std_dev": 15420.50,
                "average_rating": 6.8,
                "min_rating": 1.0,
                "max_rating": 10.0,
                "rating_std_dev": 1.4,
                "ranking_algorithm": {
                    "type": "composite_scoring",
                    "weights": {
                        "cost_effectiveness": 0.4,
                        "quality_rating": 0.35,
                        "distance_preference": 0.15,
                        "volume_experience": 0.1
                    }
                },
                "search_features": {
                    "enhanced_drg_matching": "Medical synonyms and fuzzy matching",
                    "intent_detection": "Automatic optimization for different query types"
                },
                "states_covered": ["NY"],
                "zip_code_coverage": 245
            }
        }

class ExamplesResponse(BaseModel):
    """Enhanced examples response with intent categorization"""
    examples: List[str] = Field(..., description="List of example questions for the AI assistant")
    examples_by_intent: Dict[str, List[str]] = Field(..., description="Examples categorized by intent")
    intents_supported: List[str] = Field(..., description="List of supported query intents")
    ranking_explanation: str = Field(..., description="Explanation of ranking system")
    
    class Config:
        json_schema_extra = {
            "example": {
                "examples": [
                    "Who is the cheapest for DRG 470 within 25 miles of 10001?",
                    "What are the best rated hospitals for heart surgery in New York?",
                    "Show me the best value hospitals for knee replacement near Manhattan"
                ],
                "examples_by_intent": {
                    "cheapest": ["Who is the cheapest for knee replacement?"],
                    "best_rated": ["What are the best rated hospitals for heart surgery?"],
                    "value": ["Show me the best value hospitals for knee replacement?"]
                },
                "intents_supported": ["cheapest", "best_rated", "nearest", "value"],
                "ranking_explanation": "System automatically detects intent and optimizes ranking accordingly"
            }
        }

class ErrorResponse(BaseModel):
    """Enhanced error response model with better debugging"""
    detail: str = Field(..., description="Error message")
    error_code: Optional[str] = Field(None, description="Specific error code")
    error_type: Optional[str] = Field(None, description="Type of error (validation, database, etc.)")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Error timestamp")
    request_id: Optional[str] = Field(None, description="Request ID for tracking")
    suggestions: Optional[List[str]] = Field(None, description="Suggested fixes or alternatives")
    
    class Config:
        json_schema_extra = {
            "example": {
                "detail": "Invalid ZIP code format",
                "error_code": "VALIDATION_ERROR_ZIP",
                "error_type": "validation",
                "timestamp": "2024-01-15T10:30:00Z",
                "request_id": "req_123456789",
                "suggestions": [
                    "Use 5-digit format like 10001",
                    "Include optional +4 extension like 10001-1234"
                ]
            }
        }

class RatingResponse(BaseModel):
    """Response model for individual ratings with enhanced metadata"""
    id: int = Field(..., description="Rating ID")
    provider_id: str = Field(..., description="Provider ID")
    rating: float = Field(..., ge=1.0, le=10.0, description="Rating value (1-10)")
    category: str = Field(..., description="Rating category")
    created_at: Optional[datetime] = Field(None, description="When rating was created")
    
    @validator('rating')
    def round_rating(cls, v):
        """Round rating to 1 decimal place"""
        return round(float(v), 1)
    
    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "id": 1,
                "provider_id": "330123",
                "rating": 8.5,
                "category": "overall",
                "created_at": "2024-01-15T10:30:00Z"
            }
        }

class ProviderDetailResponse(ProviderResponse):
    """Extended provider response with comprehensive rating details"""
    ratings: List[RatingResponse] = Field(default_factory=list, description="All ratings for this provider")
    rating_summary: Optional[Dict[str, float]] = Field(None, description="Rating summary by category")
    cost_percentile: Optional[float] = Field(None, ge=0, le=100, description="Cost percentile (0-100, lower is cheaper)")
    volume_percentile: Optional[float] = Field(None, ge=0, le=100, description="Volume percentile (higher means more experience)")
    
    @validator('cost_percentile', 'volume_percentile')
    def round_percentiles(cls, v):
        """Round percentiles to 1 decimal place"""
        if v is not None:
            return round(float(v), 1)
        return v
    
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
                        "category": "patient_safety"
                    }
                ],
                "rating_summary": {
                    "overall": 8.5,
                    "patient_safety": 9.0,
                    "effectiveness": 8.2
                },
                "cost_percentile": 25.5,
                "volume_percentile": 78.2
            }
        }

class SearchFilters(BaseModel):
    """Advanced search filters for complex queries"""
    min_rating: Optional[float] = Field(None, ge=1.0, le=10.0, description="Minimum rating filter")
    max_cost: Optional[float] = Field(None, ge=0, description="Maximum cost filter")
    min_volume: Optional[int] = Field(None, ge=0, description="Minimum procedure volume filter")
    hospital_types: Optional[List[str]] = Field(None, description="Filter by hospital types")
    specialties: Optional[List[str]] = Field(None, description="Filter by medical specialties")
    
    @validator('min_rating')
    def validate_min_rating(cls, v):
        if v is not None and (v < 1.0 or v > 10.0):
            raise ValueError("Minimum rating must be between 1.0 and 10.0")
        return v
    
    @validator('max_cost')
    def validate_max_cost(cls, v):
        if v is not None and v < 0:
            raise ValueError("Maximum cost must be non-negative")
        return v

# Input validation utilities
class ValidationUtils:
    """Enhanced utility functions for input validation"""
    
    @staticmethod
    def is_valid_zip_code(zip_code: str) -> bool:
        """Check if ZIP code format is valid"""
        if not zip_code:
            return False
        zip_pattern = r'^\d{5}(-\d{4})?$'
        return bool(re.match(zip_pattern, zip_code.strip()))
    
    @staticmethod
    def is_valid_drg_code(drg: str) -> bool:
        """Check if DRG code format is valid"""
        if not drg:
            return False
        # DRG codes are typically 3 digits, but allow broader patterns
        drg_pattern = r'^\d{1,4}$'
        return bool(re.match(drg_pattern, drg.strip()))
    
    @staticmethod
    def clean_zip_code(zip_code: str) -> str:
        """Clean and standardize ZIP code format"""
        if not zip_code:
            return ""
        return zip_code.strip().split('-')[0]
    
    @staticmethod
    def format_currency(amount: float) -> str:
        """Format amount as currency"""
        return f"${amount:,.2f}"
    
    @staticmethod
    def format_rating(rating: float) -> str:
        """Format rating with /10 suffix"""
        return f"{rating:.1f}/10"
    
    @staticmethod
    def extract_drg_code(drg_definition: str) -> Optional[str]:
        """Extract DRG code from definition string"""
        if not drg_definition:
            return None
        
        # Look for 3-4 digit codes at the beginning
        match = re.match(r'^(\d{1,4})\s*[-:]?\s*', drg_definition.strip())
        return match.group(1) if match else None
    
    @staticmethod
    def validate_coordinates(lat: float, lng: float) -> bool:
        """Validate geographic coordinates"""
        return -90 <= lat <= 90 and -180 <= lng <= 180
    
    @staticmethod
    def sanitize_search_term(term: str) -> str:
        """Sanitize search terms for SQL safety"""
        if not term:
            return ""
        
        # Remove potentially dangerous characters
        sanitized = re.sub(r'[<>"\';\\]', '', term.strip())
        
        # Limit length
        return sanitized[:200]
    
    @staticmethod
    def calculate_value_score(cost: float, rating: float, distance: float, volume: int) -> float:
        """Calculate composite value score using the same algorithm as backend"""
        try:
            import math
            
            # Normalize inputs
            cost = max(cost, 1000)
            rating = rating or 5.0
            distance = distance or 50
            volume = volume or 0
            
            # Calculate component scores
            cost_score = 1000000 / cost
            rating_score = rating * 15
            distance_score = max(0, 100 - (distance * 1.5))
            volume_score = min(math.log(volume + 1) * 10, 50)
            
            # Weighted composite score
            composite_score = (
                cost_score * 0.4 +
                rating_score * 0.35 +
                distance_score * 0.15 +
                volume_score * 0.1
            )
            
            return round(composite_score, 1)
            
        except Exception:
            return 0.0

# Response wrapper for consistent API responses
class APIResponse(BaseModel):
    """Generic API response wrapper"""
    success: bool = Field(..., description="Whether the request was successful")
    data: Optional[Any] = Field(None, description="Response data")
    error: Optional[ErrorResponse] = Field(None, description="Error details if unsuccessful")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")
    
    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "data": {"providers": []},
                "error": None,
                "metadata": {
                    "total_results": 25,
                    "search_time_ms": 150,
                    "ranking_mode": "value"
                }
            }
        }