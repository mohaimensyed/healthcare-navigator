from sqlalchemy import Column, Integer, String, Float, ForeignKey, Index, Text, CheckConstraint
from sqlalchemy.orm import relationship
from app.database import Base

class Provider(Base):
    __tablename__ = "providers"
    
    id = Column(Integer, primary_key=True, index=True)
    provider_id = Column(String(20), nullable=False, index=True)  # CMS ID (Rndrng_Prvdr_CCN)
    provider_name = Column(String(200), nullable=False, index=True)
    provider_city = Column(String(100), nullable=False)
    provider_state = Column(String(2), nullable=False)
    provider_zip_code = Column(String(10), nullable=False, index=True)
    ms_drg_definition = Column(Text, nullable=False, index=True)
    total_discharges = Column(Integer, nullable=False, default=0)
    average_covered_charges = Column(Float, nullable=False, default=0.0, index=True)
    average_total_payments = Column(Float, nullable=False, default=0.0)
    average_medicare_payments = Column(Float, nullable=False, default=0.0)
    
    # Geographic coordinates for radius calculations
    latitude = Column(Float, nullable=True, index=True)
    longitude = Column(Float, nullable=True, index=True)
    
    # Relationship to ratings
    ratings = relationship("Rating", back_populates="provider", cascade="all, delete-orphan")
    
    # Enhanced indexing for better search performance
    __table_args__ = (
        # Composite unique constraint for provider_id + drg combination
        Index('idx_provider_drg_unique', 'provider_id', 'ms_drg_definition', unique=True),
        
        # Performance indexes for search queries
        Index('idx_drg_text_search', 'ms_drg_definition'),  # Full text search on DRG
        Index('idx_zip_search', 'provider_zip_code'),  # ZIP code searches
        Index('idx_cost_sort', 'average_covered_charges'),  # Cost sorting
        Index('idx_location_search', 'latitude', 'longitude'),  # Geographic searches
        Index('idx_state_city', 'provider_state', 'provider_city'),  # Location filtering
        Index('idx_provider_lookup', 'provider_id'),  # Provider ID lookups
        Index('idx_name_search', 'provider_name'),  # Name searches
        
        # Composite indexes for common query patterns
        Index('idx_drg_cost', 'ms_drg_definition', 'average_covered_charges'),  # DRG + cost queries
        Index('idx_zip_cost', 'provider_zip_code', 'average_covered_charges'),  # ZIP + cost queries
        Index('idx_state_drg', 'provider_state', 'ms_drg_definition'),  # State + DRG queries
        
        # Data validity constraints
        CheckConstraint('latitude >= -90 AND latitude <= 90', name='check_latitude'),
        CheckConstraint('longitude >= -180 AND longitude <= 180', name='check_longitude'),
        CheckConstraint('average_covered_charges >= 0', name='check_covered_charges'),
        CheckConstraint('average_total_payments >= 0', name='check_total_payments'),
        CheckConstraint('average_medicare_payments >= 0', name='check_medicare_payments'),
        CheckConstraint('total_discharges >= 0', name='check_discharges'),
    )

class Rating(Base):
    __tablename__ = "ratings"
    
    id = Column(Integer, primary_key=True, index=True)
    # Reference the internal auto-increment ID to avoid foreign key constraint issues
    provider_internal_id = Column(Integer, ForeignKey("providers.id", ondelete="CASCADE"), 
                                 nullable=False, index=True)
    provider_id = Column(String(20), nullable=False, index=True)  # Store provider_id for easy querying
    rating = Column(Float, nullable=False)  # 1-10 scale as per coding exercise
    category = Column(String(50), nullable=False, default='overall')  # Rating category
    
    # Back reference to provider
    provider = relationship("Provider", back_populates="ratings")
    
    __table_args__ = (
        # Optimized indexes for rating queries
        Index('idx_provider_rating_lookup', 'provider_id', 'rating'),  # Provider + rating queries
        Index('idx_category_rating', 'category', 'rating'),  # Category-based searches
        Index('idx_provider_category', 'provider_id', 'category'),  # Provider category lookups
        Index('idx_rating_sort', 'rating'),  # Rating sorting
        Index('idx_internal_provider', 'provider_internal_id'),  # Internal FK queries
        
        # Ensure rating values are within valid range (1-10 as per exercise)
        CheckConstraint('rating >= 1.0 AND rating <= 10.0', name='check_rating_range'),
    )