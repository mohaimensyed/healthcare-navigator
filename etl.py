import asyncio
import pandas as pd
import random
import requests
import time
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from app.models import Provider, Rating, Base
import os
from dotenv import load_dotenv
from typing import Optional, Tuple

load_dotenv()

class HealthcareETL:
    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        self.engine = create_async_engine(os.getenv("DATABASE_URL"), echo=False)
        self.AsyncSessionLocal = sessionmaker(self.engine, class_=AsyncSession, expire_on_commit=False)
        
        # ZIP code to coordinates mapping for common NY ZIP codes
        self.ny_zip_coords = {
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

    async def init_database(self):
        """Initialize database tables"""
        print("Initializing database...")
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
            await conn.run_sync(Base.metadata.create_all)
        print("Database tables created successfully")

    def load_and_clean_data(self):
        """Load CSV and clean the data"""
        print(f"Loading data from {self.csv_path}")
        
        try:
            # Read CSV with different encodings if needed
            try:
                df = pd.read_csv(self.csv_path, encoding='utf-8')
            except UnicodeDecodeError:
                df = pd.read_csv(self.csv_path, encoding='latin-1')
            
            print(f"Loaded {len(df)} rows with columns: {df.columns.tolist()}")
            
            # Standardize column names (remove extra spaces, handle variations)
            df.columns = df.columns.str.strip()
            
            # Create a mapping of possible column names to our standard names
            column_mapping = {}
            standard_columns = {
                'provider_id': ['Provider Id', 'Provider ID', 'provider_id', 'Rndrng_Prvdr_CCN'],
                'provider_name': ['Provider Name', 'provider_name', 'Rndrng_Prvdr_Org_Name'],
                'provider_city': ['Provider City', 'provider_city', 'Rndrng_Prvdr_City'],
                'provider_state': ['Provider State', 'provider_state', 'Rndrng_Prvdr_State_Abrvtn'],
                'provider_zip_code': ['Provider Zip Code', 'Provider ZIP Code', 'provider_zip_code', 'Rndrng_Prvdr_Zip5'],
                'ms_drg_definition': ['DRG Definition', 'MS-DRG Definition', 'ms_drg_definition', 'DRG_Cd', 'MS_DRG_Desc'],
                'total_discharges': ['Total Discharges', 'total_discharges', 'Tot_Dschrgs'],
                'average_covered_charges': ['Average Covered Charges', 'average_covered_charges', 'Avg_Covered_Chrgs'],
                'average_total_payments': ['Average Total Payments', 'average_total_payments', 'Avg_Tot_Pymt_Amt'],
                'average_medicare_payments': ['Average Medicare Payments', 'average_medicare_payments', 'Avg_Mdcr_Pymt_Amt']
            }
            
            # Find matching columns
            for standard_name, possible_names in standard_columns.items():
                for possible_name in possible_names:
                    if possible_name in df.columns:
                        column_mapping[possible_name] = standard_name
                        break
            
            print(f"Column mapping: {column_mapping}")
            
            # Rename columns
            df = df.rename(columns=column_mapping)
            
            # Check if we have all required columns
            required_columns = ['provider_id', 'provider_name', 'provider_city', 'provider_state', 'provider_zip_code']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")
            
            # Clean the data
            initial_count = len(df)
            
            # Remove rows with missing essential data
            df = df.dropna(subset=required_columns)
            
            # Clean and convert numeric columns
            numeric_columns = ['total_discharges', 'average_covered_charges', 'average_total_payments', 'average_medicare_payments']
            
            for col in numeric_columns:
                if col in df.columns:
                    # Remove dollar signs, commas, and convert to numeric
                    df[col] = df[col].astype(str).str.replace(r'[\$,]', '', regex=True)
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    df[col] = df[col].fillna(0)
            
            # Clean ZIP codes (remove +4 extensions)
            df['provider_zip_code'] = df['provider_zip_code'].astype(str).str.split('-').str[0].str.strip()
            
            # Clean provider IDs
            df['provider_id'] = df['provider_id'].astype(str).str.strip()
            
            # Handle DRG definition - if we have separate code and description columns, combine them
            if 'ms_drg_definition' not in df.columns:
                drg_code_cols = [col for col in df.columns if 'drg' in col.lower() and 'cd' in col.lower()]
                drg_desc_cols = [col for col in df.columns if 'drg' in col.lower() and 'desc' in col.lower()]
                
                if drg_code_cols and drg_desc_cols:
                    df['ms_drg_definition'] = df[drg_code_cols[0]].astype(str) + ' - ' + df[drg_desc_cols[0]].astype(str)
                elif drg_code_cols:
                    df['ms_drg_definition'] = df[drg_code_cols[0]].astype(str)
                elif drg_desc_cols:
                    df['ms_drg_definition'] = df[drg_desc_cols[0]].astype(str)
                else:
                    df['ms_drg_definition'] = 'Unknown DRG'
            
            # Remove duplicates based on provider_id and DRG
            df = df.drop_duplicates(subset=['provider_id', 'ms_drg_definition'])
            
            print(f"After cleaning: {len(df)} rows (removed {initial_count - len(df)} rows)")
            
            # Show sample of cleaned data
            print("\nSample cleaned data:")
            print(df[['provider_id', 'provider_name', 'provider_city', 'provider_zip_code', 'ms_drg_definition']].head(3))
            
            return df
            
        except Exception as e:
            print(f"Error loading/cleaning data: {e}")
            return None

    def get_coordinates(self, zip_code: str, city: str, state: str) -> Tuple[Optional[float], Optional[float]]:
        """Get coordinates for a location using ZIP code lookup and fallback methods"""
        
        # First try our pre-calculated NY ZIP codes
        if zip_code in self.ny_zip_coords:
            return self.ny_zip_coords[zip_code]
        
        # For other ZIP codes, use approximate coordinates based on city/region
        # This avoids hitting geocoding APIs and rate limits
        
        # NYC area approximation
        if city.upper() in ['NEW YORK', 'MANHATTAN', 'BROOKLYN', 'BRONX', 'QUEENS', 'STATEN ISLAND']:
            return (40.7128 + random.uniform(-0.2, 0.2), -74.0060 + random.uniform(-0.2, 0.2))
        
        # Long Island
        if 'ISLAND' in city.upper() or zip_code.startswith('11'):
            return (40.7891 + random.uniform(-0.3, 0.3), -73.1350 + random.uniform(-0.3, 0.3))
        
        # Albany area
        if city.upper() in ['ALBANY', 'SCHENECTADY', 'TROY'] or zip_code.startswith('12'):
            return (42.6526 + random.uniform(-0.2, 0.2), -73.7562 + random.uniform(-0.2, 0.2))
        
        # Buffalo area
        if city.upper() in ['BUFFALO', 'NIAGARA FALLS'] or zip_code.startswith('14'):
            return (42.8864 + random.uniform(-0.2, 0.2), -78.8784 + random.uniform(-0.2, 0.2))
        
        # Syracuse area
        if city.upper() == 'SYRACUSE' or zip_code.startswith('13'):
            return (43.0481 + random.uniform(-0.2, 0.2), -76.1474 + random.uniform(-0.2, 0.2))
        
        # Rochester area
        if city.upper() == 'ROCHESTER' or zip_code.startswith('14'):
            return (43.1566 + random.uniform(-0.2, 0.2), -77.6088 + random.uniform(-0.2, 0.2))
        
        # Default to approximate NY state center with some randomization
        return (42.9538 + random.uniform(-2.0, 2.0), -75.5268 + random.uniform(-2.0, 2.0))

    async def process_providers(self, df: pd.DataFrame):
        """Process and insert provider data"""
        providers = []
        provider_ids = set()
        
        print("Processing providers...")
        
        for idx, row in df.iterrows():
            if idx % 1000 == 0:
                print(f"Processing provider {idx}/{len(df)}")
            
            # Get coordinates
            lat, lng = self.get_coordinates(
                row['provider_zip_code'], 
                row['provider_city'], 
                row['provider_state']
            )
            
            provider_data = {
                'provider_id': row['provider_id'],
                'provider_name': row['provider_name'],
                'provider_city': row['provider_city'],
                'provider_state': row['provider_state'],
                'provider_zip_code': row['provider_zip_code'],
                'ms_drg_definition': row.get('ms_drg_definition', 'Unknown DRG'),
                'total_discharges': int(row.get('total_discharges', 0)),
                'average_covered_charges': float(row.get('average_covered_charges', 0)),
                'average_total_payments': float(row.get('average_total_payments', 0)),
                'average_medicare_payments': float(row.get('average_medicare_payments', 0)),
                'latitude': lat,
                'longitude': lng
            }
            
            providers.append(Provider(**provider_data))
            provider_ids.add(row['provider_id'])
        
        # Batch insert providers
        print("Inserting providers into database...")
        batch_size = 500
        
        for i in range(0, len(providers), batch_size):
            batch = providers[i:i + batch_size]
            async with self.AsyncSessionLocal() as session:
                try:
                    session.add_all(batch)
                    await session.commit()
                except Exception as e:
                    print(f"Error inserting batch {i//batch_size + 1}: {e}")
                    await session.rollback()
            
            if i % (batch_size * 5) == 0:  # Progress update every 5 batches
                print(f"Inserted batch {i//batch_size + 1}/{(len(providers)-1)//batch_size + 1}")
        
        print(f"Inserted {len(providers)} provider records")
        return list(provider_ids)

    async def generate_mock_ratings(self, provider_ids: list):
        """Generate realistic mock ratings for providers"""
        if not provider_ids:
            print("No provider IDs provided for rating generation")
            return
            
        ratings = []
        categories = ['overall', 'cardiac', 'orthopedic', 'emergency', 'surgical', 'maternity']
        
        print(f"Generating mock ratings for {len(provider_ids)} unique providers...")
        
        for provider_id in provider_ids:
            # Generate 2-4 ratings per provider across different categories
            num_ratings = random.randint(2, 4)
            selected_categories = random.sample(categories, min(num_ratings, len(categories)))
            
            for category in selected_categories:
                # Generate realistic ratings (normal distribution around 7.5)
                rating = max(1.0, min(10.0, random.gauss(7.5, 1.2)))
                ratings.append(Rating(
                    provider_id=provider_id,
                    rating=round(rating, 1),
                    category=category
                ))
        
        # Batch insert ratings
        print("Inserting ratings into database...")
        batch_size = 1000
        
        for i in range(0, len(ratings), batch_size):
            batch = ratings[i:i + batch_size]
            async with self.AsyncSessionLocal() as session:
                try:
                    session.add_all(batch)
                    await session.commit()
                except Exception as e:
                    print(f"Error inserting rating batch {i//batch_size + 1}: {e}")
                    await session.rollback()
        
        print(f"Generated and inserted {len(ratings)} ratings")

    async def verify_data(self):
        """Verify that data was loaded correctly"""
        async with self.AsyncSessionLocal() as session:
            from sqlalchemy import text
            
            # Count providers
            result = await session.execute(text("SELECT COUNT(*) FROM providers"))
            provider_count = result.scalar()
            
            # Count ratings
            result = await session.execute(text("SELECT COUNT(*) FROM ratings"))
            rating_count = result.scalar()
            
            # Sample provider
            result = await session.execute(text("SELECT provider_name, provider_city, ms_drg_definition FROM providers LIMIT 3"))
            samples = result.fetchall()
            
            print(f"\n=== Data Verification ===")
            print(f"Total providers: {provider_count}")
            print(f"Total ratings: {rating_count}")
            print(f"Sample providers:")
            for sample in samples:
                print(f"  - {sample[0]} in {sample[1]} - {sample[2]}")

    async def run_etl(self):
        """Run the complete ETL process"""
        print("Starting Healthcare ETL process...")
        
        try:
            # 1. Initialize database
            await self.init_database()
            
            # 2. Load and clean data
            df = self.load_and_clean_data()
            if df is None or df.empty:
                print("❌ Failed to load data")
                return False
            
            # 3. Process providers
            provider_ids = await self.process_providers(df)
            if not provider_ids:
                print("❌ No providers were processed")
                return False
            
            # 4. Generate mock ratings
            await self.generate_mock_ratings(provider_ids)
            
            # 5. Verify data
            await self.verify_data()
            
            print("✅ ETL process completed successfully!")
            return True
            
        except Exception as e:
            print(f"❌ ETL process failed: {e}")
            return False
        finally:
            await self.engine.dispose()

async def main():
    """Main function to run the ETL process"""
    import sys
    
    csv_path = "data/sample_prices_ny.csv"
    if len(sys.argv) > 1:
        csv_path = sys.argv[1]
    
    if not os.path.exists(csv_path):
        print(f"❌ CSV file not found: {csv_path}")
        print("Please ensure the CSV file exists in the data/ directory")
        return
    
    etl = HealthcareETL(csv_path)
    success = await etl.run_etl()
    
    if success:
        print("\n🎉 Ready to start the FastAPI application!")
        print("Run: uvicorn app.main:app --reload")
    else:
        print("\n❌ ETL process failed. Please check the errors above.")

if __name__ == "__main__":
    asyncio.run(main())