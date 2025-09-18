from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, or_, text
from sqlalchemy.orm import selectinload
from typing import List, Optional
import math
import logging

from app.models import Provider, Rating
from app.schemas import ProviderResponse

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ProviderService:
    
    # Common ZIP codes and their coordinates for fast lookup
    ZIP_COORDINATES = {
        '10001': (40.7505, -73.9934), '10002': (40.7156, -73.9877), '10003': (40.7318, -73.9884),
        '10004': (40.6892, -73.9823), '10005': (40.7062, -74.0087), '10006': (40.7093, -74.0132),
        '10007': (40.7133, -74.0070), '10009': (40.7264, -73.9786), '10010': (40.7392, -73.9817),
        '10011': (40.7403, -73.9965), '10012': (40.7258, -73.9986), '10013': (40.7197, -74.0026),
        '10014': (40.7342, -74.0063), '10016': (40.7453, -73.9781), '10017': (40.7520, -73.9717),
        '10018': (40.7549, -73.9925), '10019': (40.7663, -73.9896), '10021': (40.7697, -73.9540),
        '10022': (40.7590, -73.9719), '10023': (40.7756, -73.9822), '10024': (40.7876, -73.9754),
        '10025': (40.7982, -73.9665), '10026': (40.8020, -73.9532), '10027': (40.8115, -73.9534),
        '10028': (40.7774, -73.9533), '10029': (40.7917, -73.9439), '10030': (40.8200, -73.9425),
        '10031': (40.8251, -73.9490), '10032': (40.8386, -73.9426), '10033': (40.8501, -73.9344),
        '10034': (40.8677, -73.9212), '10035': (40.7957, -73.9389), '10036': (40.7590, -73.9845),
        '10037': (40.8142, -73.9370), '10038': (40.7095, -74.0021), '10039': (40.8203, -73.9370),
        '10040': (40.8677, -73.9212), '10065': (40.7641, -73.9630), '10075': (40.7733, -73.9565),
        '10128': (40.7816, -73.9509), '10280': (40.7081, -74.0173)
    }
    
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
        try:
            logger.info(f"Searching providers: DRG={drg}, ZIP={zip_code}, Radius={radius_km}km")
            
            # Get coordinates for the search ZIP code
            search_lat, search_lng = await self._get_zip_coordinates(db, zip_code)
            if not search_lat or not search_lng:
                logger.warning(f"Could not find coordinates for ZIP {zip_code}")
                # Fallback to NYC coordinates if ZIP not found
                search_lat, search_lng = 40.7128, -74.0060
            
            logger.info(f"Search coordinates: {search_lat}, {search_lng}")
            
            # Build the query with DRG matching and ratings
            query = (
                select(Provider, func.avg(Rating.rating).label('avg_rating'))
                .outerjoin(Rating, Provider.provider_id == Rating.provider_id)
                .group_by(Provider.id)
            )
            
            # DRG matching - handle both numeric codes and text descriptions
            drg_conditions = []
            
            if drg.isdigit():
                # If it's a number, match DRG code at the beginning
                drg_conditions.append(Provider.ms_drg_definition.ilike(f"{drg} %"))
                drg_conditions.append(Provider.ms_drg_definition.ilike(f"{drg}-%"))
            else:
                # If it's text, do fuzzy matching on description
                drg_words = drg.split()
                for word in drg_words:
                    if len(word) > 2:  # Skip very short words
                        drg_conditions.append(Provider.ms_drg_definition.ilike(f"%{word}%"))
            
            if drg_conditions:
                query = query.where(or_(*drg_conditions))
            
            # Execute query to get all matching providers
            result = await db.execute(query)
            providers_with_ratings = result.all()
            
            logger.info(f"Found {len(providers_with_ratings)} providers matching DRG criteria")
            
            # Filter by radius and calculate distances
            filtered_providers = []
            for provider, avg_rating in providers_with_ratings:
                if provider.latitude and provider.longitude:
                    distance = self._calculate_distance(
                        search_lat, search_lng,
                        provider.latitude, provider.longitude
                    )
                    
                    if distance <= radius_km:
                        provider_response = ProviderResponse(
                            provider_id=provider.provider_id,
                            provider_name=provider.provider_name,
                            provider_city=provider.provider_city,
                            provider_state=provider.provider_state,
                            provider_zip_code=provider.provider_zip_code,
                            ms_drg_definition=provider.ms_drg_definition,
                            total_discharges=provider.total_discharges or 0,
                            average_covered_charges=provider.average_covered_charges or 0.0,
                            average_total_payments=provider.average_total_payments or 0.0,
                            average_medicare_payments=provider.average_medicare_payments or 0.0,
                            average_rating=round(avg_rating, 1) if avg_rating else None,
                            distance_km=round(distance, 2)
                        )
                        filtered_providers.append(provider_response)
            
            logger.info(f"Found {len(filtered_providers)} providers within {radius_km}km radius")
            
            # Sort by average covered charges (cheapest first)
            filtered_providers.sort(key=lambda x: x.average_covered_charges or 0)
            
            return filtered_providers[:limit]
            
        except Exception as e:
            logger.error(f"Error in search_providers: {e}")
            return []
    
    async def _get_zip_coordinates(self, db: AsyncSession, zip_code: str) -> tuple:
        """Get coordinates for a ZIP code from multiple sources"""
        
        # First, check our hardcoded coordinates
        if zip_code in self.ZIP_COORDINATES:
            return self.ZIP_COORDINATES[zip_code]
        
        try:
            # Then, check existing providers in database
            query = select(Provider.latitude, Provider.longitude).where(
                Provider.provider_zip_code == zip_code
            ).limit(1)
            
            result = await db.execute(query)
            coords = result.first()
            
            if coords and coords.latitude and coords.longitude:
                return coords.latitude, coords.longitude
        
        except Exception as e:
            logger.error(f"Error getting ZIP coordinates from database: {e}")
        
        # Fallback: approximate coordinates based on ZIP code pattern
        if zip_code.startswith('10'):  # NYC area
            return 40.7128, -74.0060
        elif zip_code.startswith('11'):  # Long Island
            return 40.7891, -73.1350
        elif zip_code.startswith('12'):  # Albany area
            return 42.6526, -73.7562
        elif zip_code.startswith('13'):  # Syracuse area
            return 43.0481, -76.1474
        elif zip_code.startswith('14'):  # Buffalo/Rochester area
            return 43.0481, -77.6088
        else:
            # Default to NY state center
            return 42.9538, -75.5268
    
    def _calculate_distance(self, lat1: float, lng1: float, lat2: float, lng2: float) -> float:
        """Calculate distance between two points using Haversine formula"""
        try:
            # Haversine formula
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
            
        except Exception as e:
            logger.error(f"Error calculating distance: {e}")
            return float('inf')  # Return infinite distance if calculation fails
    
    async def get_provider_by_id(self, db: AsyncSession, provider_id: str) -> Optional[Provider]:
        """Get a provider by ID with ratings"""
        try:
            query = select(Provider).options(selectinload(Provider.ratings)).where(
                Provider.provider_id == provider_id
            )
            result = await db.execute(query)
            return result.scalars().first()
        except Exception as e:
            logger.error(f"Error getting provider by ID: {e}")
            return None
    
    async def get_top_rated_providers(
        self, 
        db: AsyncSession, 
        drg: str = None, 
        limit: int = 10
    ) -> List[ProviderResponse]:
        """Get top rated providers, optionally filtered by DRG"""
        
        try:
            # Build query with ratings
            query = (
                select(Provider, func.avg(Rating.rating).label('avg_rating'))
                .join(Rating, Provider.provider_id == Rating.provider_id)
                .group_by(Provider.id)
                .having(func.count(Rating.id) >= 1)  # Ensure they have at least 1 rating
                .order_by(func.avg(Rating.rating).desc())
                .limit(limit)
            )
            
            if drg:
                drg_conditions = []
                if drg.isdigit():
                    drg_conditions.append(Provider.ms_drg_definition.ilike(f"{drg} %"))
                    drg_conditions.append(Provider.ms_drg_definition.ilike(f"{drg}-%"))
                else:
                    drg_words = drg.split()
                    for word in drg_words:
                        if len(word) > 2:
                            drg_conditions.append(Provider.ms_drg_definition.ilike(f"%{word}%"))
                
                if drg_conditions:
                    query = query.where(or_(*drg_conditions))
            
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
                    total_discharges=provider.total_discharges or 0,
                    average_covered_charges=provider.average_covered_charges or 0.0,
                    average_total_payments=provider.average_total_payments or 0.0,
                    average_medicare_payments=provider.average_medicare_payments or 0.0,
                    average_rating=round(avg_rating, 1) if avg_rating else None
                )
                provider_responses.append(provider_response)
            
            return provider_responses
            
        except Exception as e:
            logger.error(f"Error getting top rated providers: {e}")
            return []
    
    async def get_cheapest_providers(
        self,
        db: AsyncSession,
        drg: str = None,
        limit: int = 10
    ) -> List[ProviderResponse]:
        """Get the cheapest providers, optionally filtered by DRG"""
        
        try:
            query = (
                select(Provider, func.avg(Rating.rating).label('avg_rating'))
                .outerjoin(Rating, Provider.provider_id == Rating.provider_id)
                .group_by(Provider.id)
                .where(Provider.average_covered_charges > 0)  # Exclude $0 charges
                .order_by(Provider.average_covered_charges.asc())
                .limit(limit)
            )
            
            if drg:
                drg_conditions = []
                if drg.isdigit():
                    drg_conditions.append(Provider.ms_drg_definition.ilike(f"{drg} %"))
                    drg_conditions.append(Provider.ms_drg_definition.ilike(f"{drg}-%"))
                else:
                    drg_words = drg.split()
                    for word in drg_words:
                        if len(word) > 2:
                            drg_conditions.append(Provider.ms_drg_definition.ilike(f"%{word}%"))
                
                if drg_conditions:
                    query = query.where(or_(*drg_conditions))
            
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
                    total_discharges=provider.total_discharges or 0,
                    average_covered_charges=provider.average_covered_charges or 0.0,
                    average_total_payments=provider.average_total_payments or 0.0,
                    average_medicare_payments=provider.average_medicare_payments or 0.0,
                    average_rating=round(avg_rating, 1) if avg_rating else None
                )
                provider_responses.append(provider_response)
            
            return provider_responses
            
        except Exception as e:
            logger.error(f"Error getting cheapest providers: {e}")
            return []
    
    async def get_provider_statistics(self, db: AsyncSession) -> dict:
        """Get basic statistics about the provider database"""
        try:
            stats = {}
            
            # Total providers
            provider_count_query = select(func.count(Provider.id.distinct()))
            result = await db.execute(provider_count_query)
            stats['total_providers'] = result.scalar()
            
            # Total unique DRGs
            drg_count_query = select(func.count(Provider.ms_drg_definition.distinct()))
            result = await db.execute(drg_count_query)
            stats['total_drgs'] = result.scalar()
            
            # Average cost
            avg_cost_query = select(func.avg(Provider.average_covered_charges))
            result = await db.execute(avg_cost_query)
            stats['average_cost'] = round(result.scalar() or 0, 2)
            
            # Total ratings
            rating_count_query = select(func.count(Rating.id))
            result = await db.execute(rating_count_query)
            stats['total_ratings'] = result.scalar()
            
            # Average rating
            avg_rating_query = select(func.avg(Rating.rating))
            result = await db.execute(avg_rating_query)
            stats['average_rating'] = round(result.scalar() or 0, 1)
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting provider statistics: {e}")
            return {}