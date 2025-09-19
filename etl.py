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
from typing import Optional, Tuple, Dict, List
import math
import logging

load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HealthcareETL:
    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        self.engine = create_async_engine(os.getenv("DATABASE_URL"), echo=False)
        self.AsyncSessionLocal = sessionmaker(self.engine, class_=AsyncSession, expire_on_commit=False)
        
        # Enhanced ZIP code to coordinates mapping for better NYC coverage
        self.ny_zip_coords = {
            # Manhattan
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
            '10128': (40.7816, -73.9509), '10280': (40.7081, -74.0173),
            
            # Brooklyn
            '11201': (40.6938, -73.9901), '11215': (40.6693, -73.9864), '11217': (40.6839, -73.9677),
            '11218': (40.6441, -73.9761), '11219': (40.6184, -73.9956), '11220': (40.6416, -74.0174),
            '11221': (40.6911, -73.9279), '11222': (40.7286, -73.9469), '11223': (40.5967, -73.9732),
            '11224': (40.5786, -73.9885), '11225': (40.6596, -73.9538), '11226': (40.6467, -73.9562),
            '11229': (40.6008, -73.9444), '11230': (40.6206, -73.9645), '11235': (40.5883, -73.9495),
            '11237': (40.7040, -73.9201),
            
            # Queens  
            '11101': (40.7505, -73.9364), '11102': (40.7717, -73.9262), '11103': (40.7616, -73.9140),
            '11104': (40.7442, -73.9206), '11105': (40.7789, -73.9057), '11106': (40.7615, -73.9298),
            '11354': (40.7690, -73.8366), '11355': (40.7499, -73.8226), '11356': (40.7849, -73.8483),
            '11357': (40.7901, -73.8273), '11358': (40.7603, -73.7991), '11360': (40.7823, -73.7824),
            
            # Bronx
            '10451': (40.8205, -73.9253), '10452': (40.8407, -73.9242), '10453': (40.8518, -73.9112),
            '10454': (40.8094, -73.9180), '10455': (40.8088, -73.8976), '10456': (40.8276, -73.9104),
            '10457': (40.8502, -73.8977), '10458': (40.8631, -73.8885), '10459': (40.8266, -73.8934),
            '10460': (40.8424, -73.8786), '10461': (40.8484, -73.8328), '10462': (40.8404, -73.8613),
            '10463': (40.8795, -73.9114), '10464': (40.8439, -73.7855), '10465': (40.8267, -73.8287),
            '10466': (40.8897, -73.8503), '10467': (40.8736, -73.8780), '10468': (40.8676, -73.9006),
            '10469': (40.8688, -73.8463), '10470': (40.8958, -73.8634), '10471': (40.8958, -73.8985),
            '10472': (40.8294, -73.8608), '10473': (40.8176, -73.8479), '10474': (40.8098, -73.8878),
            '10475': (40.8786, -73.8249),
            
            # Staten Island
            '10301': (40.6436, -74.0834), '10302': (40.6280, -74.1348), '10303': (40.6346, -74.1754),
            '10304': (40.6093, -74.0864), '10305': (40.5971, -74.0654), '10306': (40.5667, -74.1224),
            '10307': (40.5048, -74.2414), '10308': (40.5560, -74.1510), '10309': (40.5296, -74.2368),
            '10310': (40.6306, -74.1152), '10311': (40.6077, -74.1764), '10312': (40.5489, -74.1827),
            '10314': (40.5988, -74.1618)
        }
        
        # Enhanced hospital quality patterns for more realistic ratings
        self.hospital_quality_patterns = {
            # Top-tier hospitals (typically get 8-10 ratings)
            'special_surgery': {'base_rating': 8.5, 'variance': 1.0},
            'mount_sinai': {'base_rating': 8.2, 'variance': 1.2},
            'nyu_langone': {'base_rating': 8.3, 'variance': 1.1},
            'presbyterian': {'base_rating': 8.0, 'variance': 1.3},
            'memorial_sloan': {'base_rating': 9.0, 'variance': 0.8},
            'weill_cornell': {'base_rating': 8.4, 'variance': 1.0},
            
            # Mid-tier hospitals (typically get 6-8 ratings)
            'lenox_hill': {'base_rating': 7.5, 'variance': 1.2},
            'beth_israel': {'base_rating': 7.2, 'variance': 1.3},
            'brooklyn_hospital': {'base_rating': 6.8, 'variance': 1.4},
            'jamaica_hospital': {'base_rating': 6.5, 'variance': 1.5},
            
            # Community hospitals (typically get 5-7 ratings)
            'community_hospital': {'base_rating': 6.0, 'variance': 1.5},
            'general_hospital': {'base_rating': 6.2, 'variance': 1.4},
            
            # Default for unknown hospitals
            'default': {'base_rating': 6.5, 'variance': 1.8}
        }
        
        # Procedure-specific rating modifiers
        self.procedure_modifiers = {
            # Hospitals known for specific specialties get rating boosts
            'cardiac': {
                'mount_sinai': 0.5, 'presbyterian': 0.7, 'nyu_langone': 0.4
            },
            'orthopedic': {
                'special_surgery': 1.0, 'mount_sinai': 0.3, 'nyu_langone': 0.2
            },
            'cancer': {
                'memorial_sloan': 1.2, 'mount_sinai': 0.4, 'nyu_langone': 0.3
            },
            'emergency': {
                'bellevue': 0.5, 'presbyterian': 0.3
            }
        }

    async def init_database(self):
        """Initialize database tables with proper constraints"""
        logger.info("Initializing database...")
        try:
            async with self.engine.begin() as conn:
                await conn.run_sync(Base.metadata.drop_all)
                logger.info("Dropped existing tables")
                await conn.run_sync(Base.metadata.create_all)
                logger.info("Created new tables with enhanced schema")
        except Exception as e:
            logger.error(f"Error initializing database: {e}")
            raise

    def load_and_clean_data(self):
        """Enhanced data loading with better error handling and validation"""
        logger.info(f"Loading data from {self.csv_path}")
        
        try:
            # Try multiple encodings
            encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
            df = None
            
            for encoding in encodings:
                try:
                    df = pd.read_csv(self.csv_path, encoding=encoding)
                    logger.info(f"Successfully loaded with {encoding} encoding")
                    break
                except UnicodeDecodeError:
                    continue
            
            if df is None:
                raise ValueError("Could not read CSV file with any supported encoding")
            
            logger.info(f"Loaded {len(df)} rows with columns: {df.columns.tolist()}")
            
            # Standardize column names
            df.columns = df.columns.str.strip()
            
            # Enhanced column mapping for various CMS file formats
            column_mapping = self._create_column_mapping(df.columns)
            logger.info(f"Column mapping: {column_mapping}")
            
            # Apply column mapping
            df = df.rename(columns=column_mapping)
            
            # Validate required columns
            required_columns = ['provider_id', 'provider_name', 'provider_city', 'provider_state', 'provider_zip_code']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")
            
            # Clean and validate data
            df = self._clean_data(df)
            
            logger.info(f"After cleaning: {len(df)} rows")
            logger.info(f"Sample data:\n{df[['provider_name', 'provider_city', 'ms_drg_definition']].head()}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading/cleaning data: {e}")
            raise

    def _create_column_mapping(self, columns: List[str]) -> Dict[str, str]:
        """Create intelligent column mapping for various CMS file formats"""
        column_mapping = {}
        
        # Standard mappings with variations
        standard_columns = {
            'provider_id': [
                'Provider Id', 'Provider ID', 'provider_id', 'Rndrng_Prvdr_CCN',
                'CMS_Certification_Number', 'CCN'
            ],
            'provider_name': [
                'Provider Name', 'provider_name', 'Rndrng_Prvdr_Org_Name',
                'Hospital_Name', 'Organization_Name'
            ],
            'provider_city': [
                'Provider City', 'provider_city', 'Rndrng_Prvdr_City',
                'City', 'Hospital_City'
            ],
            'provider_state': [
                'Provider State', 'provider_state', 'Rndrng_Prvdr_State_Abrvtn',
                'State', 'St', 'State_Code'
            ],
            'provider_zip_code': [
                'Provider Zip Code', 'Provider ZIP Code', 'provider_zip_code',
                'Rndrng_Prvdr_Zip5', 'ZIP_Code', 'Zip', 'Postal_Code'
            ],
            'ms_drg_definition': [
                'DRG Definition', 'MS-DRG Definition', 'ms_drg_definition',
                'DRG_Cd', 'MS_DRG_Desc', 'DRG_Description', 'Procedure_Description'
            ],
            'total_discharges': [
                'Total Discharges', 'total_discharges', 'Tot_Dschrgs',
                'Discharges', 'Total_Cases'
            ],
            'average_covered_charges': [
                'Average Covered Charges', 'average_covered_charges', 'Avg_Covered_Chrgs',
                'Avg_Submtd_Cvrd_Chrg', 'Average_Charges', 'Hospital_Charges'
            ],
            'average_total_payments': [
                'Average Total Payments', 'average_total_payments', 'Avg_Tot_Pymt_Amt',
                'Total_Payments', 'Average_Payment'
            ],
            'average_medicare_payments': [
                'Average Medicare Payments', 'average_medicare_payments', 'Avg_Mdcr_Pymt_Amt',
                'Medicare_Payments', 'Medicare_Amount'
            ]
        }
        
        # Find best matches
        for standard_name, possible_names in standard_columns.items():
            for possible_name in possible_names:
                if possible_name in columns:
                    column_mapping[possible_name] = standard_name
                    break
        
        return column_mapping

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enhanced data cleaning with validation"""
        initial_count = len(df)
        
        # Remove rows with missing essential data
        required_columns = ['provider_id', 'provider_name', 'provider_city', 'provider_state', 'provider_zip_code']
        available_required = [col for col in required_columns if col in df.columns]
        df = df.dropna(subset=available_required)
        
        # Clean and convert numeric columns
        numeric_columns = ['total_discharges', 'average_covered_charges', 'average_total_payments', 'average_medicare_payments']
        
        for col in numeric_columns:
            if col in df.columns:
                # Remove currency symbols and convert to numeric
                df[col] = df[col].astype(str).str.replace(r'[\$,]', '', regex=True)
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].fillna(0)
        
        # Clean ZIP codes (remove +4 extensions)
        df['provider_zip_code'] = df['provider_zip_code'].astype(str).str.split('-').str[0].str.strip()
        
        # Clean provider IDs and names
        df['provider_id'] = df['provider_id'].astype(str).str.strip()
        df['provider_name'] = df['provider_name'].astype(str).str.strip()
        
        # Handle DRG definition
        if 'ms_drg_definition' not in df.columns:
            df['ms_drg_definition'] = 'Unknown DRG'
        
        # Validate data ranges
        if 'average_covered_charges' in df.columns:
            df = df[df['average_covered_charges'] >= 0]
        
        # Remove duplicates
        df = df.drop_duplicates(subset=['provider_id', 'ms_drg_definition'])
        
        logger.info(f"Cleaned data: removed {initial_count - len(df)} invalid rows")
        return df

    def get_coordinates(self, zip_code: str, city: str, state: str) -> Tuple[Optional[float], Optional[float]]:
        """Enhanced coordinate lookup with better regional coverage"""
        
        # First check our comprehensive ZIP code database
        if zip_code in self.ny_zip_coords:
            return self.ny_zip_coords[zip_code]
        
        # Enhanced regional approximations
        city_upper = city.upper()
        
        # NYC boroughs with better accuracy
        if any(borough in city_upper for borough in ['NEW YORK', 'MANHATTAN']):
            base_lat, base_lng = 40.7589, -73.9851
            return (base_lat + random.uniform(-0.05, 0.05), base_lng + random.uniform(-0.05, 0.05))
        elif 'BROOKLYN' in city_upper:
            base_lat, base_lng = 40.6782, -73.9442
            return (base_lat + random.uniform(-0.1, 0.1), base_lng + random.uniform(-0.1, 0.1))
        elif 'BRONX' in city_upper:
            base_lat, base_lng = 40.8448, -73.8648
            return (base_lat + random.uniform(-0.1, 0.1), base_lng + random.uniform(-0.1, 0.1))
        elif 'QUEENS' in city_upper:
            base_lat, base_lng = 40.7282, -73.7949
            return (base_lat + random.uniform(-0.15, 0.15), base_lng + random.uniform(-0.15, 0.15))
        elif 'STATEN ISLAND' in city_upper:
            base_lat, base_lng = 40.5795, -74.1502
            return (base_lat + random.uniform(-0.1, 0.1), base_lng + random.uniform(-0.1, 0.1))
        
        # Other NY regions with ZIP-based approximation
        elif zip_code.startswith('11'):  # Long Island
            return (40.7891 + random.uniform(-0.3, 0.3), -73.1350 + random.uniform(-0.3, 0.3))
        elif zip_code.startswith('12'):  # Albany area
            return (42.6526 + random.uniform(-0.2, 0.2), -73.7562 + random.uniform(-0.2, 0.2))
        elif zip_code.startswith('13'):  # Syracuse area
            return (43.0481 + random.uniform(-0.2, 0.2), -76.1474 + random.uniform(-0.2, 0.2))
        elif zip_code.startswith('14'):  # Buffalo/Rochester area
            return (43.0481 + random.uniform(-0.2, 0.2), -77.6088 + random.uniform(-0.2, 0.2))
        
        # Default to NY state center with random variation
        return (42.9538 + random.uniform(-2.0, 2.0), -75.5268 + random.uniform(-2.0, 2.0))

    async def process_providers(self, df: pd.DataFrame):
        """Enhanced provider processing with better error handling"""
        providers = []
        provider_ids = set()
        
        logger.info("Processing providers...")
        
        for idx, row in df.iterrows():
            if idx % 1000 == 0:
                logger.info(f"Processing provider {idx+1}/{len(df)}")
            
            try:
                # Get coordinates
                lat, lng = self.get_coordinates(
                    row['provider_zip_code'], 
                    row['provider_city'], 
                    row['provider_state']
                )
                
                provider_data = {
                    'provider_id': str(row['provider_id']),
                    'provider_name': str(row['provider_name']),
                    'provider_city': str(row['provider_city']),
                    'provider_state': str(row['provider_state']),
                    'provider_zip_code': str(row['provider_zip_code']),
                    'ms_drg_definition': str(row.get('ms_drg_definition', 'Unknown DRG')),
                    'total_discharges': max(0, int(row.get('total_discharges', 0))),
                    'average_covered_charges': max(0.0, float(row.get('average_covered_charges', 0))),
                    'average_total_payments': max(0.0, float(row.get('average_total_payments', 0))),
                    'average_medicare_payments': max(0.0, float(row.get('average_medicare_payments', 0))),
                    'latitude': lat,
                    'longitude': lng
                }
                
                providers.append(Provider(**provider_data))
                provider_ids.add(str(row['provider_id']))
                
            except Exception as e:
                logger.warning(f"Error processing row {idx}: {e}")
                continue
        
        # Batch insert providers
        logger.info(f"Inserting {len(providers)} providers into database...")
        batch_size = 500
        
        for i in range(0, len(providers), batch_size):
            batch = providers[i:i + batch_size]
            async with self.AsyncSessionLocal() as session:
                try:
                    session.add_all(batch)
                    await session.commit()
                except Exception as e:
                    logger.error(f"Error inserting provider batch {i//batch_size + 1}: {e}")
                    await session.rollback()
                    # Try individual inserts for this batch
                    for provider in batch:
                        try:
                            async with self.AsyncSessionLocal() as single_session:
                                single_session.add(provider)
                                await single_session.commit()
                        except Exception as single_error:
                            logger.warning(f"Failed to insert provider {provider.provider_name}: {single_error}")
            
            if i % (batch_size * 5) == 0:
                logger.info(f"Inserted batch {i//batch_size + 1}/{(len(providers)-1)//batch_size + 1}")
        
        logger.info(f"Successfully inserted providers for {len(provider_ids)} unique provider IDs")
        return list(provider_ids)

    def _identify_hospital_category(self, provider_name: str) -> str:
        """Identify hospital category for realistic rating generation"""
        name_lower = provider_name.lower()
        
        # Top-tier hospitals
        if any(term in name_lower for term in ['special surgery', 'hospital for special']):
            return 'special_surgery'
        elif any(term in name_lower for term in ['mount sinai', 'mt sinai']):
            return 'mount_sinai'
        elif any(term in name_lower for term in ['nyu', 'langone']):
            return 'nyu_langone'
        elif any(term in name_lower for term in ['presbyterian', 'columbia']):
            return 'presbyterian'
        elif any(term in name_lower for term in ['memorial', 'sloan', 'kettering']):
            return 'memorial_sloan'
        elif any(term in name_lower for term in ['weill', 'cornell']):
            return 'weill_cornell'
        
        # Mid-tier hospitals
        elif any(term in name_lower for term in ['lenox hill']):
            return 'lenox_hill'
        elif any(term in name_lower for term in ['beth israel']):
            return 'beth_israel'
        elif any(term in name_lower for term in ['brooklyn hospital', 'bk hospital']):
            return 'brooklyn_hospital'
        elif any(term in name_lower for term in ['jamaica hospital']):
            return 'jamaica_hospital'
        
        # Community hospitals
        elif any(term in name_lower for term in ['community', 'neighborhood']):
            return 'community_hospital'
        elif any(term in name_lower for term in ['general hospital', 'general medical']):
            return 'general_hospital'
        
        return 'default'

    def _identify_procedure_category(self, drg_definition: str) -> str:
        """Identify procedure category for specialty rating modifiers"""
        drg_lower = drg_definition.lower()
        
        if any(term in drg_lower for term in ['heart', 'cardiac', 'coronary', 'cardiovascular']):
            return 'cardiac'
        elif any(term in drg_lower for term in ['joint', 'knee', 'hip', 'orthopedic', 'replacement']):
            return 'orthopedic'
        elif any(term in drg_lower for term in ['cancer', 'oncology', 'tumor', 'malignancy']):
            return 'cancer'
        elif any(term in drg_lower for term in ['emergency', 'trauma', 'urgent']):
            return 'emergency'
        
        return 'general'

    async def generate_enhanced_mock_ratings(self, provider_ids: List[str]):
        """Generate realistic mock ratings based on hospital reputation and specialties"""
        if not provider_ids:
            logger.warning("No provider IDs provided for rating generation")
            return
        
        logger.info(f"Generating enhanced mock ratings for {len(provider_ids)} providers...")
        
        # Get provider details for intelligent rating generation
        provider_details = {}
        async with self.AsyncSessionLocal() as session:
            try:
                from sqlalchemy import select
                result = await session.execute(
                    select(Provider.id, Provider.provider_id, Provider.provider_name, Provider.ms_drg_definition)
                )
                for internal_id, provider_id, name, drg in result:
                    if provider_id not in provider_details:
                        provider_details[provider_id] = {
                            'internal_id': internal_id,
                            'name': name, 
                            'procedures': []
                        }
                    provider_details[provider_id]['procedures'].append(drg)
            except Exception as e:
                logger.error(f"Error fetching provider details: {e}")
                return
        
        ratings = []
        categories = ['overall', 'patient_safety', 'effectiveness', 'timeliness', 'patient_experience']
        
        for provider_id in provider_ids:
            provider_info = provider_details.get(provider_id, {'internal_id': None, 'name': 'Unknown', 'procedures': []})
            provider_name = provider_info['name']
            internal_id = provider_info['internal_id']
            
            if internal_id is None:
                logger.warning(f"Could not find internal ID for provider {provider_id}")
                continue
            
            # Determine hospital category and base ratings
            hospital_category = self._identify_hospital_category(provider_name)
            quality_pattern = self.hospital_quality_patterns.get(hospital_category, self.hospital_quality_patterns['default'])
            
            # Generate 3-5 ratings per provider across different categories
            num_ratings = random.randint(3, 5)
            selected_categories = random.sample(categories, min(num_ratings, len(categories)))
            
            for category in selected_categories:
                # Base rating from hospital reputation
                base_rating = quality_pattern['base_rating']
                variance = quality_pattern['variance']
                
                # Apply procedure-specific modifiers
                procedure_modifier = 0.0
                for drg in provider_info['procedures']:
                    procedure_category = self._identify_procedure_category(drg)
                    if procedure_category in self.procedure_modifiers:
                        modifier_dict = self.procedure_modifiers[procedure_category]
                        procedure_modifier += modifier_dict.get(hospital_category, 0.0)
                
                # Calculate final rating with bounds checking
                final_rating = base_rating + procedure_modifier + random.gauss(0, variance)
                final_rating = max(1.0, min(10.0, final_rating))  # Ensure 1-10 range per coding exercise
                
                # Add slight category-specific adjustments
                category_adjustments = {
                    'overall': 0.0,
                    'patient_safety': -0.2,  # Slightly more conservative
                    'effectiveness': 0.1,    # Slightly higher for good hospitals
                    'timeliness': -0.3,      # Often a challenge
                    'patient_experience': -0.1  # Variable
                }
                
                final_rating += category_adjustments.get(category, 0.0)
                final_rating = max(1.0, min(10.0, round(final_rating, 1)))
                
                ratings.append(Rating(
                    provider_internal_id=internal_id,  # Use the internal ID
                    provider_id=provider_id,           # Also store the provider_id for easy querying
                    rating=final_rating,
                    category=category
                ))
        
        # Batch insert ratings
        logger.info(f"Inserting {len(ratings)} ratings into database...")
        batch_size = 1000
        
        for i in range(0, len(ratings), batch_size):
            batch = ratings[i:i + batch_size]
            async with self.AsyncSessionLocal() as session:
                try:
                    session.add_all(batch)
                    await session.commit()
                except Exception as e:
                    logger.error(f"Error inserting rating batch {i//batch_size + 1}: {e}")
                    await session.rollback()
        
        logger.info(f"Successfully generated and inserted {len(ratings)} realistic mock ratings")
        
        # Log rating distribution for verification
        await self._log_rating_statistics()

    async def _log_rating_statistics(self):
        """Log rating statistics for verification"""
        try:
            async with self.AsyncSessionLocal() as session:
                from sqlalchemy import select, func
                
                # Overall rating stats
                result = await session.execute(
                    select(
                        func.count(Rating.id),
                        func.avg(Rating.rating),
                        func.min(Rating.rating),
                        func.max(Rating.rating)
                    )
                )
                count, avg_rating, min_rating, max_rating = result.first()
                
                logger.info(f"Rating Statistics:")
                logger.info(f"  Total ratings: {count}")
                logger.info(f"  Average rating: {avg_rating:.2f}")
                logger.info(f"  Rating range: {min_rating:.1f} - {max_rating:.1f}")
                
                # Rating distribution by category
                result = await session.execute(
                    select(Rating.category, func.avg(Rating.rating), func.count(Rating.id))
                    .group_by(Rating.category)
                )
                
                logger.info("Rating by category:")
                for category, avg_cat_rating, cat_count in result:
                    logger.info(f"  {category}: {avg_cat_rating:.2f} avg ({cat_count} ratings)")
                    
        except Exception as e:
            logger.error(f"Error logging rating statistics: {e}")

    async def verify_data(self):
        """Enhanced data verification with relationship checks"""
        async with self.AsyncSessionLocal() as session:
            try:
                from sqlalchemy import text
                
                # Provider statistics
                result = await session.execute(text("SELECT COUNT(*) FROM providers"))
                provider_count = result.scalar()
                
                result = await session.execute(text("SELECT COUNT(DISTINCT provider_id) FROM providers"))
                unique_providers = result.scalar()
                
                # Rating statistics
                result = await session.execute(text("SELECT COUNT(*) FROM ratings"))
                rating_count = result.scalar()
                
                result = await session.execute(text("SELECT AVG(rating), MIN(rating), MAX(rating) FROM ratings"))
                avg_rating, min_rating, max_rating = result.first()
                
                # Relationship verification
                result = await session.execute(text("""
                    SELECT COUNT(*) FROM ratings r 
                    WHERE NOT EXISTS (SELECT 1 FROM providers p WHERE p.provider_id = r.provider_id)
                """))
                orphaned_ratings = result.scalar()
                
                # Sample data
                result = await session.execute(text("""
                    SELECT p.provider_name, p.provider_city, p.ms_drg_definition, 
                           AVG(r.rating) as avg_rating
                    FROM providers p 
                    LEFT JOIN ratings r ON p.provider_id = r.provider_id
                    GROUP BY p.provider_id, p.provider_name, p.provider_city, p.ms_drg_definition
                    LIMIT 5
                """))
                samples = result.fetchall()
                
                logger.info("=== Enhanced Data Verification ===")
                logger.info(f"Provider Records: {provider_count}")
                logger.info(f"Unique Providers: {unique_providers}")
                logger.info(f"Total Ratings: {rating_count}")
                logger.info(f"Rating Range: {min_rating:.1f} - {max_rating:.1f} (avg: {avg_rating:.2f})")
                logger.info(f"Orphaned Ratings: {orphaned_ratings}")
                logger.info("Sample providers with ratings:")
                for sample in samples:
                    provider_name, city, drg, rating = sample
                    rating_str = f"{rating:.1f}" if rating else "No ratings"
                    logger.info(f"  {provider_name} ({city}) - {drg[:50]}... - Rating: {rating_str}")
                
                if orphaned_ratings > 0:
                    logger.warning(f"Found {orphaned_ratings} orphaned ratings - check foreign key constraints")
                
            except Exception as e:
                logger.error(f"Error during data verification: {e}")

    async def run_etl(self):
        """Run the complete enhanced ETL process"""
        logger.info("Starting Enhanced Healthcare ETL process...")
        
        try:
            # 1. Initialize database
            await self.init_database()
            
            # 2. Load and clean data
            df = self.load_and_clean_data()
            if df is None or df.empty:
                logger.error("Failed to load data")
                return False
            
            # 3. Process providers
            provider_ids = await self.process_providers(df)
            if not provider_ids:
                logger.error("No providers were processed")
                return False
            
            # 4. Generate enhanced mock ratings
            await self.generate_enhanced_mock_ratings(provider_ids)
            
            # 5. Verify data and relationships
            await self.verify_data()
            
            logger.info("✅ Enhanced ETL process completed successfully!")
            logger.info("🏥 Database is ready with realistic hospital rankings!")
            return True
            
        except Exception as e:
            logger.error(f"❌ ETL process failed: {e}")
            return False
        finally:
            await self.engine.dispose()

async def main():
    """Main function with enhanced error handling"""
    import sys
    
    csv_path = "data/sample_prices_ny.csv"
    if len(sys.argv) > 1:
        csv_path = sys.argv[1]
    
    if not os.path.exists(csv_path):
        logger.error(f"❌ CSV file not found: {csv_path}")
        logger.info("Please ensure the CSV file exists in the data/ directory")
        logger.info("You can create one using: python process_cms_data.py")
        return
    
    etl = HealthcareETL(csv_path)
    success = await etl.run_etl()
    
    if success:
        logger.info("\n🎉 Ready to start the FastAPI application!")
        logger.info("Run: uvicorn app.main:app --reload")
        logger.info("\n📊 Features enabled:")
        logger.info("  • Multi-factor ranking (cost + quality + distance + volume)")
        logger.info("  • Realistic mock ratings based on hospital reputation")
        logger.info("  • Enhanced geographic coverage for NYC area")
        logger.info("  • Intelligent procedure-specific quality modifiers")
    else:
        logger.error("\n❌ ETL process failed. Please check the errors above.")

if __name__ == "__main__":
    asyncio.run(main())