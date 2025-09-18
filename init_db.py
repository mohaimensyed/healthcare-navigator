import asyncio
import sys
import os
from pathlib import Path

# Add the parent directory to the path so we can import from app
sys.path.append(str(Path(__file__).parent.parent))

from app.database import engine
from app.models import Base
from sqlalchemy import text

async def init_db():
    """Initialize the database by creating all tables"""
    print("Creating database tables...")
    
    async with engine.begin() as conn:
        # Drop all tables first (be careful in production!)
        await conn.run_sync(Base.metadata.drop_all)
        print("Dropped existing tables")
        
        # Create all tables
        await conn.run_sync(Base.metadata.create_all)
        print("Created all tables successfully")
    
    print("Database initialization complete!")

async def check_db():
    """Test database connection"""
    try:
        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT 1"))
            print("✅ Database connection successful")
            return True
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False

async def main():
    print("Healthcare Cost Navigator - Database Setup")
    print("=" * 50)
    
    # Test connection first
    print("Testing database connection...")
    if await check_db():
        # Initialize database
        await init_db()
        
        # Verify tables were created
        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public'"))
            table_count = result.scalar()
            print(f"Created {table_count} tables")
        
        print("✅ Database setup completed successfully!")
        print("\nNext step: Run the ETL script to load data")
        print("Command: python etl.py")
    else:
        print("❌ Database setup failed - could not connect to database")
        print("\nCheck your DATABASE_URL in .env file")
    
    await engine.dispose()

if __name__ == "__main__":
    asyncio.run(main())