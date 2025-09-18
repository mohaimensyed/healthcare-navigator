import asyncio
import pandas as pd
import random
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import time
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from app.models import Provider, Rating, Base
import os
from dotenv import load_dotenv

load_dotenv()

class HealthcareETL:
    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        self.engine = create_async_engine(os.getenv("DATABASE_URL"), echo=False)
        self.AsyncSessionLocal = sessionmaker(self.engine, class_=AsyncSession, expire_on_commit=False)
        self.geolocator = Nominatim(user_agent="healthcare_navigator")
        
    async def init_database(self):
        """Initialize database tables"""
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
            await conn.run_sync(Base.metadata.create_all)
        print("Database tables created successfully")

    def load_and_clean_data(self):
        """Load CSV and clean the data"""
        print(f"Loading data from {self.csv_path}")
        
        # Read CSV - adjust column names based on actual CSV structure
        df = pd.read_csv(self.csv_path)
        
        # Print initial info
        print(f"Loaded {len(df)} rows")
        print("Columns:", df.columns.tolist())
        
        # Clean the data
        # Remove rows with missing critical data
        initial_count = len(df)
        df = df.dropna(subset=['Provider Name', 'Provider City', 'Provider State', 
                              'Provider Zip Code', 'DRG Definition'])
        
        # Clean numeric columns
        numeric_cols = ['Total Discharges', 'Average Covered Charges', 
                       'Average Total Payments', 'Average Medicare Payments']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace('$', '').str.replace(',', ''), errors='coerce')
        
        # Remove outliers (optional - very high or very low charges)
        if 'Average Covered Charges' in df.columns:
            q1 = df['Average Covered Charges'].quantile(0.01)
            q99 = df['Average Covered Charges'].quantile(0.99)
            df = df[(df['Average Covered Charges'] >= q1) & (df['Average Covered Charges'] <= q99)]
        
        print(f"After cleaning: {len(df)} rows (removed {initial_count - len(df)} rows)")
        return df

    def get_coordinates(self, city: str, state: str, zip_code: str):
        """Get latitude and longitude for an address"""
        try:
            # Try with ZIP code first (most accurate)
            location = self.geolocator.geocode(f"{zip_code}, {state}, USA", timeout=10)
            if location:
                return location.latitude, location.longitude
            
            # Fallback to city, state
            location = self.geolocator.geocode(f"{city}, {state}, USA", timeout=10)
            if location:
                return location.latitude, location.longitude
                
        except (GeocoderTimedOut, Exception) as e:
            print(f"Geocoding failed for {city}, {state} {zip_code}: {e}")
            
        return None, None

    async def process_providers(self, df: pd.DataFrame):
        """Process and insert provider data"""
        providers = []
        
        print("Processing providers...")
        for idx, row in df.iterrows():
            if idx % 1000 == 0:
                print(f"Processing provider {idx}/{len(df)}")
            
            # Map CSV columns to our model (adjust based on actual CSV structure)
            provider_data = {
                'provider_id': str(row.get('Provider Id', '')),
                'provider_name': row.get('Provider Name', ''),
                'provider_city': row.get('Provider City', ''),
                'provider_state': row.get('Provider State', ''),
                'provider_zip_code': str(row.get('Provider Zip Code', '')),
                'ms_drg_definition': row.get('DRG Definition', ''),
                'total_discharges': int(row.get('Total Discharges', 0)) if pd.notna(row.get('Total Discharges', 0)) else 0,
                'average_covered_charges': float(row.get('Average Covered Charges', 0)) if pd.notna(row.get('Average Covered Charges', 0)) else 0,
                'average_total_payments': float(row.get('Average Total Payments', 0)) if pd.notna(row.get('Average Total Payments', 0)) else 0,
                'average_medicare_payments': float(row.get('Average Medicare Payments', 0)) if pd.notna(row.get('Average Medicare Payments', 0)) else 0,
            }
            
            # Get coordinates (rate limited to avoid hitting API limits)
            if idx % 100 == 0:  # Only geocode every 100th address to save time
                lat, lng = self.get_coordinates(
                    provider_data['provider_city'],
                    provider_data['provider_state'],
                    provider_data['provider_zip_code']
                )
                provider_data['latitude'] = lat
                provider_data['longitude'] = lng
                time.sleep(1)  # Rate limiting
            else:
                # For demo purposes, use approximate coordinates based on ZIP
                # In production, you'd geocode all addresses or use a ZIP code database
                provider_data['latitude'] = 40.7128 + random.uniform(-0.5, 0.5)  # NYC area
                provider_data['longitude'] = -74.0060 + random.uniform(-0.5, 0.5)
            
            providers.append(Provider(**provider_data))
        
        # Batch insert providers
        async with self.AsyncSessionLocal() as session:
            session.add_all(providers)
            await session.commit()
        
        print(f"Inserted {len(providers)} providers")
        return [p.provider_id for p in providers]

    async def generate_mock_ratings(self, provider_ids: list):
        """Generate mock star ratings for providers"""
        ratings = []
        categories = ['overall', 'cardiac', 'orthopedic', 'emergency', 'surgical']
        
        print("Generating mock ratings...")
        for provider_id in provider_ids:
            # Generate 1-3 ratings per provider
            num_ratings = random.randint(1, 3)
            selected_categories = random.sample(categories, num_ratings)
            
            for category in selected_categories:
                # Generate realistic ratings (skewed towards higher ratings)
                rating = max(1, min(10, random.gauss(7.5, 1.5)))
                ratings.append(Rating(
                    provider_id=provider_id,
                    rating=round(rating, 1),
                    category=category
                ))
        
        # Batch insert ratings
        async with self.AsyncSessionLocal() as session:
            session.add_all(ratings)
            await session.commit()
        
        print(f"Generated {len(ratings)} ratings")

    async def run_etl(self):
        """Run the complete ETL process"""
        print("Starting ETL process...")
        
        # 1. Initialize database
        await self.init_database()
        
        # 2. Load and clean data
        df = self.load_and_clean_data()
        
        # 3. Process providers
        provider_ids = await self.process_providers(df)
        
        # 4. Generate mock ratings
        await self.generate_mock_ratings(provider_ids)
        
        print("ETL process completed successfully!")
        
        # Close engine
        await self.engine.dispose()

# Usage
async def main():
    etl = HealthcareETL("data/sample_prices_ny.csv")
    await etl.run_etl()

if __name__ == "__main__":
    asyncio.run(main())