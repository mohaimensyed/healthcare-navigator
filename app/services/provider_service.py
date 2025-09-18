from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, or_, text
from sqlalchemy.orm import selectinload
from typing import List, Optional
import math
from fuzzywuzzy import fuzz
from geopy.distance import geodesic

from app.models import Provider, Rating
from app.schemas import ProviderResponse

class ProviderService:
    
    async def search_providers(
        self, 
        db: AsyncSession, 
        drg: str, 
        zip_code: str, 
        radius_km: int, 
        limit: int = 50
    ) -> List[ProviderResponse]:
        """
        Search for providers by DRG and geographic location
        """
        
        # First, get coordinates for the search ZIP code
        search_lat, search_lng = await self._get_zip_coordinates(db, zip_code)
        if not search_lat or not search_lng:
            # Fallback to NYC coordinates if ZIP not found
            search_lat, search_lng = 40.7128, -74.0060
        
        # Build the query with DRG matching
        query = select(Provider).options(selectinload(Provider.ratings))
        
        # DRG matching - try exact match first, then fuzzy
        if drg.isdigit():
            # If it's a number, match DRG code
            drg_condition = Provider.ms_drg_definition.ilike(f"{drg} %")
        else:
            # If it's text, do fuzzy matching on description
            drg_condition = Provider.ms_drg_definition.ilike(f"%{drg}%")
        
        query = query.where(drg_condition)
        
        # Execute query
        result = await db.execute(query)
        providers = result.scalars().all()
        
        # Filter by radius and calculate distances
        filtered_providers = []
        for provider in providers:
            if provider.latitude and provider.longitude:
                distance = self._calculate_distance(
                    search_lat, search_lng,
                    provider.latitude, provider.longitude
                )
                if distance <= radius_km:
                    # Calculate average rating
                    avg_rating = None
                    if provider.ratings:
                        avg_rating = sum(r.rating for r in provider.ratings) / len(provider.ratings)
                    
                    provider_response = ProviderResponse(
                        provider_id=provider.provider_id,
                        provider_name=provider.provider_name,
                        provider_city=provider.provider_city,
                        provider_state=provider.provider_state,
                        provider_zip_code=provider.provider_zip_code,
                        ms_drg_definition=provider.ms_drg_definition,
                        total_discharges=provider.total_discharges,
                        average_covered_charges=provider.average_covered_charges,
                        average_total_payments=provider.average_total_payments,
                        average_medicare_payments=provider.average_medicare_payments,
                        average_rating=avg_rating,
                        distance_km=round(distance, 2)
                    )
                    filtered_providers.append(provider_response)
        
        # Sort by average covered charges (cheapest first)
        filtered_providers.sort(key=lambda x: x.average_covered_charges)
        
        return filtered_providers[:limit]
    
    async def _get_zip_coordinates(self, db: AsyncSession, zip_code: str) -> tuple:
        """Get coordinates for a ZIP code from existing providers"""
        query = select(Provider.latitude, Provider.longitude).where(
            Provider.provider_zip_code == zip_code
        ).limit(1)
        
        result = await db.execute(query)
        coords = result.first()
        
        if coords:
            return coords.latitude, coords.longitude
        return None, None
    
    def _calculate_distance(self, lat1: float, lng1: float, lat2: float, lng2: float) -> float:
        """Calculate distance between two points in kilometers"""
        try:
            return geodesic((lat1, lng1), (lat2, lng2)).kilometers
        except:
            # Fallback to haversine formula
            R = 6371  # Earth's radius in kilometers
            
            lat1_rad = math.radians(lat1)
            lat2_rad = math.radians(lat2)
            delta_lat = math.radians(lat2 - lat1)
            delta_lng = math.radians(lng2 - lng1)
            
            a = (math.sin(delta_lat/2) * math.sin(delta_lat/2) +
                 math.cos(lat1_rad) * math.cos(lat2_rad) *
                 math.sin(delta_lng/2) * math.sin(delta_lng/2))
            
            c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
            return R * c
    
    async def get_provider_by_id(self, db: AsyncSession, provider_id: str) -> Optional[Provider]:
        """Get a provider by ID"""
        query = select(Provider).options(selectinload(Provider.ratings)).where(
            Provider.provider_id == provider_id
        )
        result = await db.execute(query)
        return result.scalars().first()
    
    async def get_top_rated_providers(
        self, 
        db: AsyncSession, 
        drg: str = None, 
        limit: int = 10
    ) -> List[ProviderResponse]:
        """Get top rated providers, optionally filtered by DRG"""
        
        # Build query with ratings
        query = (
            select(Provider, func.avg(Rating.rating).label('avg_rating'))
            .join(Rating, Provider.provider_id == Rating.provider_id)
            .group_by(Provider.id)
            .order_by(func.avg(Rating.rating).desc())
            .limit(limit)
        )
        
        if drg:
            if drg.isdigit():
                drg_condition = Provider.ms_drg_definition.ilike(f"{drg} %")
            else:
                drg_condition = Provider.ms_drg_definition.ilike(f"%{drg}%")
            query = query.where(drg_condition)
        
        result = await db.execute(query)
        providers_with_ratings = result.all()
        
        provider_responses = []
        for provider, avg_rating in providers_with_ratings:
            provider_response = ProviderResponse(
                provider_id=provider.provider_id,
                provider_name=provider.provider_name,
                provider_city=provider.provider_city,
                provider_state=provider.provider_state,
                provider_zip_code=provider.provider_zip_code,
                ms_drg_definition=provider.ms_drg_definition,
                total_discharges=provider.total_discharges,
                average_covered_charges=provider.average_covered_charges,
                average_total_payments=provider.average_total_payments,
                average_medicare_payments=provider.average_medicare_payments,
                average_rating=round(avg_rating, 2) if avg_rating else None
            )
            provider_responses.append(provider_response)
        
        return provider_responses