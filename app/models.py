from sqlalchemy import Column, Integer, String, Float, ForeignKey, Index
from sqlalchemy.orm import relationship
from app.database import Base

class Provider(Base):
    __tablename__ = "providers"
    
    id = Column(Integer, primary_key=True, index=True)
    provider_id = Column(String, unique=True, index=True)  # CMS ID
    provider_name = Column(String, index=True)
    provider_city = Column(String)
    provider_state = Column(String)
    provider_zip_code = Column(String, index=True)  # For radius queries
    ms_drg_definition = Column(String, index=True)  # For DRG searches
    total_discharges = Column(Integer)
    average_covered_charges = Column(Float, index=True)  # For sorting by cost
    average_total_payments = Column(Float)
    average_medicare_payments = Column(Float)
    
    # Geographic coordinates for radius calculations
    latitude = Column(Float)
    longitude = Column(Float)
    
    # Relationship to ratings
    ratings = relationship("Rating", back_populates="provider")
    
    # Indexes for performance
    __table_args__ = (
        Index('idx_drg_zip', 'ms_drg_definition', 'provider_zip_code'),
        Index('idx_provider_cost', 'provider_id', 'average_covered_charges'),
        Index('idx_lat_lng', 'latitude', 'longitude'),
    )

class Rating(Base):
    __tablename__ = "ratings"
    
    id = Column(Integer, primary_key=True, index=True)
    provider_id = Column(String, ForeignKey("providers.provider_id"), index=True)
    rating = Column(Float)  # 1-10 scale
    category = Column(String)  # e.g., "overall", "cardiac", "orthopedic"
    
    # Relationship
    provider = relationship("Provider", back_populates="ratings")
    
    __table_args__ = (
        Index('idx_provider_rating', 'provider_id', 'rating'),
        Index('idx_category_rating', 'category', 'rating'),
    )