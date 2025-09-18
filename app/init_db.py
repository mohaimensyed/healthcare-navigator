# app/init_db.py (corrected version)
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
    
    await engine.dispose()
    print("Database initialization complete!")

async def check_db_connection():
    """Test database connection"""
    try:
        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT 1"))
            print("✅ Database connection successful")
            return True
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False

if __name__ == "__main__":
    # First check if we can connect
    print("Testing database connection...")
    
    try:
        # Test connection first
        if asyncio.run(check_db_connection()):
            # If connection works, initialize tables
            asyncio.run(init_db())
        else:
            print("Please check your database configuration in .env file")
            print("Make sure your Supabase connection string is correct")
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
    except Exception as e:
        print(f"Error during database initialization: {e}")
        print("\nTroubleshooting tips:")
        print("1. Make sure your .env file has the correct DATABASE_URL")
        print("2. Check your Supabase connection string") 
        print("3. Verify you have asyncpg installed: pip install asyncpg")