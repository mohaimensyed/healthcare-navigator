from sqlalchemy import Column, Integer, String, Float, ForeignKey, Index, Text, CheckConstraint
from sqlalchemy.orm import relationship
from app.database import Base

class Provider(Base):
    __tablename__ = "providers"
    
    id = Column(Integer, primary_key=True, index=True)
    provider_id = Column(String(20), nullable=False, index=True)  # CMS ID - NOT unique because one provider can have multiple DRGs
    provider_name = Column(String(200), nullable=False, index=True)
    provider_city = Column(String(100), nullable=False)
    provider_state = Column(String(2), nullable=False)
    provider_zip_code = Column(String(10), nullable=False, index=True)  # For radius queries
    ms_drg_definition = Column(Text, nullable=False, index=True)  # For DRG searches
    total_discharges = Column(Integer, nullable=False, default=0)
    average_covered_charges = Column(Float, nullable=False, default=0.0, index=True)  # For sorting by cost
    average_total_payments = Column(Float, nullable=False, default=0.0)
    average_medicare_payments = Column(Float, nullable=False, default=0.0)
    
    # Geographic coordinates for radius calculations
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    
    # Relationship to ratings - NOTE: This will get all ratings for this provider_id
    ratings = relationship("Rating", back_populates="provider", cascade="all, delete-orphan")
    
    # Constraints
    __table_args__ = (
        # Composite unique constraint for provider_id + drg combination (one provider can offer multiple DRGs)
        Index('idx_provider_drg_unique', 'provider_id', 'ms_drg_definition', unique=True),
        # Performance indexes
        Index('idx_drg_search', 'ms_drg_definition'),
        Index('idx_zip_search', 'provider_zip_code'),
        Index('idx_cost_sort', 'average_covered_charges'),
        Index('idx_location', 'latitude', 'longitude'),
        Index('idx_state_city', 'provider_state', 'provider_city'),
        # Check constraints for data validity
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
    # Reference the internal ID instead of provider_id to avoid foreign key constraint issues
    provider_internal_id = Column(Integer, ForeignKey("providers.id", ondelete="CASCADE"), nullable=False, index=True)
    provider_id = Column(String(20), nullable=False, index=True)  # Store provider_id for easy querying
    rating = Column(Float, nullable=False)  # 1-10 scale
    category = Column(String(50), nullable=False, default='overall')  # e.g., "overall", "cardiac", "orthopedic"
    
    # Relationship
    provider = relationship("Provider", back_populates="ratings")
    
    __table_args__ = (
        # Composite index for efficient rating queries
        Index('idx_provider_rating', 'provider_id', 'rating'),
        Index('idx_category_rating', 'category', 'rating'),
        Index('idx_provider_category', 'provider_id', 'category'),
        # Check constraint for rating range
        CheckConstraint('rating >= 1.0 AND rating <= 10.0', name='check_rating_range'),
    )