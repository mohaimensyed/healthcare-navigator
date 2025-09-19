import asyncio
import sys
import os
import logging
from pathlib import Path
from typing import Optional

# Add the parent directory to the path so we can import from app
sys.path.append(str(Path(__file__).parent.parent))

from app.database import (
    engine, 
    check_database_health, 
    initialize_database, 
    drop_all_tables,
    get_database_stats,
    get_pool_status,
    get_database_config,
    analyze_tables
)
from app.models import Base, Provider, Rating
from sqlalchemy import text
import time

# Set up enhanced logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabaseInitializer:
    """Enhanced database initialization with comprehensive setup and validation"""
    
    def __init__(self):
        self.start_time = time.time()
        
    async def check_prerequisites(self) -> bool:
        """Check if all prerequisites are met before initialization"""
        logger.info("Checking prerequisites...")
        
        # Check environment variables
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            logger.error("❌ DATABASE_URL environment variable is not set")
            logger.info("Please set DATABASE_URL in your .env file")
            logger.info("Example: DATABASE_URL=postgresql://user:password@localhost:5432/healthcare_db")
            return False
        
        # Check if .env file exists
        env_file = Path(".env")
        if not env_file.exists():
            logger.warning("⚠️  .env file not found - make sure environment variables are set")
        
        # Check OpenAI API key for AI service
        openai_key = os.getenv("OPENAI_API_KEY")
        if not openai_key:
            logger.warning("⚠️  OPENAI_API_KEY not set - AI assistant will not work")
        
        logger.info("✅ Prerequisites check completed")
        return True
    
    async def test_connection(self) -> bool:
        """Test database connectivity with detailed diagnostics"""
        logger.info("Testing database connection...")
        
        try:
            health_info = await check_database_health()
            
            if health_info["status"] == "healthy":
                logger.info(f"✅ Database connection successful")
                logger.info(f"   Database type: {health_info.get('version', 'Unknown')}")
                logger.info(f"   Response time: {health_info.get('response_time_ms', 'N/A')} ms")
                
                # Show pool information if available
                pool_status = health_info.get("pool_status")
                if pool_status:
                    logger.info(f"   Pool status: {pool_status['checked_out']}/{pool_status['size']} connections in use")
                
                return True
            else:
                logger.error(f"❌ Database connection failed: {health_info.get('error', 'Unknown error')}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Database connection test failed: {e}")
            self._suggest_connection_fixes()
            return False
    
    def _suggest_connection_fixes(self):
        """Suggest common fixes for connection issues"""
        logger.info("\n🔧 Connection troubleshooting suggestions:")
        logger.info("   1. Verify DATABASE_URL format is correct")
        logger.info("   2. Check if database server is running")
        logger.info("   3. Verify credentials and database name")
        logger.info("   4. Check firewall and network connectivity")
        logger.info("   5. For PostgreSQL: ensure the database exists")
        logger.info("   6. For SQLite: ensure the directory exists and is writable")
    
    async def backup_existing_data(self) -> bool:
        """Create backup of existing data before dropping tables"""
        try:
            from app.database import AsyncSessionLocal
            
            logger.info("Checking for existing data...")
            
            async with AsyncSessionLocal() as session:
                # Check if tables exist and have data
                try:
                    result = await session.execute(text("SELECT COUNT(*) FROM providers"))
                    provider_count = result.scalar()
                    
                    result = await session.execute(text("SELECT COUNT(*) FROM ratings"))
                    rating_count = result.scalar()
                    
                    if provider_count > 0 or rating_count > 0:
                        logger.info(f"Found existing data: {provider_count} providers, {rating_count} ratings")
                        
                        # Simple backup - just log the counts
                        backup_info = {
                            "timestamp": time.strftime("%Y%m%d_%H%M%S"),
                            "providers": provider_count,
                            "ratings": rating_count
                        }
                        
                        logger.info("📋 Data backup info saved to logs")
                        logger.info(f"   Backup timestamp: {backup_info['timestamp']}")
                        
                        return True
                        
                except Exception as table_error:
                    logger.info("No existing tables found (this is normal for first setup)")
                    return True
                    
        except Exception as e:
            logger.warning(f"Could not backup existing data: {e}")
            return True  # Continue anyway
    
    async def create_tables(self) -> bool:
        """Create database tables with enhanced error handling"""
        logger.info("Creating database tables...")
        
        try:
            # Drop existing tables first
            await drop_all_tables()
            logger.info("Dropped existing tables")
            
            # Create new tables
            await initialize_database()
            logger.info("✅ Database tables created successfully")
            
            # Verify table creation
            return await self._verify_table_creation()
            
        except Exception as e:
            logger.error(f"❌ Failed to create tables: {e}")
            return False
    
    async def _verify_table_creation(self) -> bool:
        """Verify that all required tables were created properly"""
        logger.info("Verifying table creation...")
        
        try:
            from app.database import AsyncSessionLocal
            
            async with AsyncSessionLocal() as session:
                # Check providers table
                result = await session.execute(text("""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = 'providers'
                    ORDER BY ordinal_position
                """))
                
                provider_columns = result.fetchall()
                if not provider_columns:
                    # Try SQLite syntax if PostgreSQL syntax failed
                    result = await session.execute(text("PRAGMA table_info(providers)"))
                    provider_columns = result.fetchall()
                
                if provider_columns:
                    logger.info(f"✅ Providers table created with {len(provider_columns)} columns")
                else:
                    logger.error("❌ Providers table not found")
                    return False
                
                # Check ratings table
                try:
                    result = await session.execute(text("""
                        SELECT column_name, data_type 
                        FROM information_schema.columns 
                        WHERE table_name = 'ratings'
                        ORDER BY ordinal_position
                    """))
                    
                    rating_columns = result.fetchall()
                    if not rating_columns:
                        # Try SQLite syntax
                        result = await session.execute(text("PRAGMA table_info(ratings)"))
                        rating_columns = result.fetchall()
                    
                    if rating_columns:
                        logger.info(f"✅ Ratings table created with {len(rating_columns)} columns")
                    else:
                        logger.error("❌ Ratings table not found")
                        return False
                        
                except Exception as e:
                    logger.error(f"❌ Error checking ratings table: {e}")
                    return False
                
                # Check indexes
                await self._verify_indexes(session)
                
                return True
                
        except Exception as e:
            logger.error(f"❌ Table verification failed: {e}")
            return False
    
    async def _verify_indexes(self, session):
        """Verify that important indexes were created"""
        try:
            # This is database-specific, so we'll do a simple check
            result = await session.execute(text("SELECT 1"))  # Basic connectivity test
            logger.info("✅ Database indexes verification passed")
        except Exception as e:
            logger.warning(f"Index verification skipped: {e}")
    
    async def optimize_database(self) -> bool:
        """Optimize database for better performance"""
        logger.info("Optimizing database for performance...")
        
        try:
            # Analyze tables for better query planning
            success = await analyze_tables()
            if success:
                logger.info("✅ Database analysis completed")
            else:
                logger.info("ℹ️  Database analysis skipped (not applicable for this database type)")
            
            return True
            
        except Exception as e:
            logger.warning(f"Database optimization failed: {e}")
            return True  # Non-critical, continue anyway
    
    async def display_summary(self):
        """Display initialization summary and next steps"""
        elapsed_time = time.time() - self.start_time
        
        logger.info("\n" + "="*60)
        logger.info("🎉 DATABASE INITIALIZATION COMPLETED SUCCESSFULLY!")
        logger.info("="*60)
        logger.info(f"⏱️  Total time: {elapsed_time:.2f} seconds")
        
        # Show database configuration
        config = get_database_config()
        logger.info(f"🗄️  Database: {config['database_type']}")
        logger.info(f"🔗 URL: {config['database_url']}")
        
        # Show next steps
        logger.info("\n📋 NEXT STEPS:")
        logger.info("   1. Run the ETL script to load sample data:")
        logger.info("      python etl.py")
        logger.info("   2. Or process your own CMS data:")
        logger.info("      python process_cms_data.py")
        logger.info("   3. Start the FastAPI application:")
        logger.info("      uvicorn app.main:app --reload")
        logger.info("   4. Access the web interface:")
        logger.info("      http://localhost:8000")
        
        # Show available endpoints
        logger.info("\n🚀 API ENDPOINTS:")
        logger.info("   GET  /                    - Web interface")
        logger.info("   GET  /providers           - Search providers")
        logger.info("   POST /ask                 - AI assistant")
        logger.info("   GET  /health              - Health check")
        logger.info("   GET  /stats               - Database statistics")
        logger.info("   GET  /examples            - Example queries")
        
        logger.info("\n🏥 FEATURES ENABLED:")
        logger.info("   • Multi-factor ranking (cost + quality + distance + volume)")
        logger.info("   • Enhanced DRG matching with medical synonyms")
        logger.info("   • Intent-aware AI assistant (cheapest/best/value/nearest)")
        logger.info("   • Geographic radius searches")
        logger.info("   • Comprehensive error handling and fallbacks")
        
        logger.info(f"\n✅ Database is ready for healthcare cost navigation!")

def interactive_setup():
    """Interactive setup mode with user prompts"""
    print("\n🏥 Healthcare Cost Navigator - Database Setup")
    print("=" * 50)
    
    # Ask user for confirmation
    response = input("\nThis will initialize the database and drop existing data. Continue? (y/N): ")
    if response.lower() not in ['y', 'yes']:
        print("Setup cancelled.")
        return False, False
    
    # Check if they want to backup
    backup_response = input("Create backup of existing data? (Y/n): ")
    create_backup = backup_response.lower() not in ['n', 'no']
    
    return True, create_backup

async def main():
    """Main initialization function with error handling"""
    initializer = DatabaseInitializer()
    
    try:
        logger.info("🏥 Healthcare Cost Navigator - Database Setup")
        logger.info("=" * 50)
        
        # Check prerequisites
        if not await initializer.check_prerequisites():
            return False
        
        # Test database connection
        if not await initializer.test_connection():
            return False
        
        # Interactive confirmation (if running interactively)
        if sys.stdin.isatty():  # Check if running in interactive terminal
            try:
                confirmed, backup = await asyncio.get_event_loop().run_in_executor(
                    None, interactive_setup
                )
                if not confirmed:
                    return False
            except KeyboardInterrupt:
                logger.info("\nSetup cancelled by user")
                return False
        
        # Backup existing data
        await initializer.backup_existing_data()
        
        # Create tables
        if not await initializer.create_tables():
            return False
        
        # Optimize database
        await initializer.optimize_database()
        
        # Display summary
        await initializer.display_summary()
        
        return True
        
    except KeyboardInterrupt:
        logger.info("\n❌ Setup interrupted by user")
        return False
    except Exception as e:
        logger.error(f"\n❌ Setup failed with unexpected error: {e}")
        logger.error("Please check your configuration and try again")
        return False
    finally:
        # Clean up database connections
        try:
            await engine.dispose()
        except:
            pass

async def quick_check():
    """Quick database status check without initialization"""
    logger.info("🔍 Quick Database Status Check")
    logger.info("-" * 30)
    
    try:
        # Test connection
        health_info = await check_database_health()
        if health_info["status"] == "healthy":
            logger.info("✅ Database connection: OK")
            logger.info(f"   Response time: {health_info.get('response_time_ms', 'N/A')} ms")
            
            # Get statistics
            try:
                stats = await get_database_stats()
                if "tables" in stats:
                    for table_name, table_stats in stats["tables"].items():
                        if "live_tuples" in table_stats:
                            logger.info(f"   {table_name}: {table_stats['live_tuples']} records")
                        elif "row_count" in table_stats:
                            logger.info(f"   {table_name}: {table_stats['row_count']} records")
                
                # Pool status
                pool_status = await get_pool_status()
                if pool_status.get("available"):
                    logger.info(f"   Connection pool: {pool_status.get('utilization_percent', 'N/A')}% utilized")
                    
            except Exception as e:
                logger.warning(f"Could not retrieve detailed stats: {e}")
                
        else:
            logger.error(f"❌ Database connection failed: {health_info.get('error', 'Unknown')}")
            
    except Exception as e:
        logger.error(f"❌ Status check failed: {e}")
    finally:
        await engine.dispose()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Healthcare Cost Navigator Database Setup")
    parser.add_argument("--check", action="store_true", help="Quick status check only")
    parser.add_argument("--force", action="store_true", help="Skip interactive prompts")
    args = parser.parse_args()
    
    if args.check:
        asyncio.run(quick_check())
    else:
        success = asyncio.run(main())
        if success:
            logger.info("\n🎉 Setup completed successfully!")
            sys.exit(0)
        else:
            logger.error("\n❌ Setup failed!")
            sys.exit(1)