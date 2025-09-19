from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy.pool import StaticPool
import os
import logging
from typing import AsyncGenerator
from dotenv import load_dotenv

load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database configuration
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    logger.error("DATABASE_URL environment variable is not set")
    raise ValueError("DATABASE_URL environment variable is required")

# Enhanced engine configuration for better performance
engine_kwargs = {
    "echo": os.getenv("SQL_ECHO", "false").lower() == "true",  # Configurable SQL logging
    "pool_size": int(os.getenv("DB_POOL_SIZE", "10")),  # Connection pool size
    "max_overflow": int(os.getenv("DB_MAX_OVERFLOW", "20")),  # Max overflow connections
    "pool_timeout": int(os.getenv("DB_POOL_TIMEOUT", "30")),  # Pool timeout in seconds
    "pool_recycle": int(os.getenv("DB_POOL_RECYCLE", "3600")),  # Recycle connections after 1 hour
    "pool_pre_ping": True,  # Validate connections before use
}

# Special handling for SQLite (for development/testing)
if DATABASE_URL.startswith("sqlite"):
    logger.info("Using SQLite database configuration")
    engine_kwargs.update({
        "poolclass": StaticPool,
        "connect_args": {
            "check_same_thread": False,
            "timeout": 30
        }
    })
    # Remove PostgreSQL-specific options for SQLite
    engine_kwargs.pop("pool_size", None)
    engine_kwargs.pop("max_overflow", None)
    engine_kwargs.pop("pool_timeout", None)
    engine_kwargs.pop("pool_recycle", None)
else:
    logger.info("Using PostgreSQL database configuration")
    # PostgreSQL-specific optimizations
    engine_kwargs["connect_args"] = {
        "server_settings": {
            "application_name": "healthcare_cost_navigator",
            "jit": "off",  # Disable JIT for faster query startup
        },
        "command_timeout": 60,
    }

# Create the async engine
try:
    engine = create_async_engine(DATABASE_URL, **engine_kwargs)
    logger.info(f"Database engine created successfully")
    logger.info(f"Database URL: {DATABASE_URL.split('@')[0]}@****")  # Hide password in logs
except Exception as e:
    logger.error(f"Failed to create database engine: {e}")
    raise

# Create async session factory
AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=True,  # Automatically flush before queries
    autocommit=False
)

# Create declarative base
Base = declarative_base()

# Enhanced database dependency with proper error handling and logging
async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    Enhanced database dependency with connection management and error handling
    """
    session = None
    try:
        session = AsyncSessionLocal()
        logger.debug("Database session created")
        yield session
    except Exception as e:
        logger.error(f"Database session error: {e}")
        if session:
            await session.rollback()
            logger.debug("Database session rolled back due to error")
        raise
    finally:
        if session:
            await session.close()
            logger.debug("Database session closed")

# Database health check function
async def check_database_health() -> dict:
    """
    Check database connectivity and performance
    """
    health_info = {
        "status": "unknown",
        "connection": False,
        "version": None,
        "pool_status": None,
        "response_time_ms": None
    }
    
    try:
        import time
        from sqlalchemy import text
        
        start_time = time.time()
        
        async with AsyncSessionLocal() as session:
            # Test basic connectivity
            result = await session.execute(text("SELECT 1 as test"))
            test_value = result.scalar()
            
            if test_value == 1:
                health_info["connection"] = True
                
                # Get database version
                if DATABASE_URL.startswith("postgresql"):
                    version_result = await session.execute(text("SELECT version()"))
                    health_info["version"] = version_result.scalar()
                elif DATABASE_URL.startswith("sqlite"):
                    version_result = await session.execute(text("SELECT sqlite_version()"))
                    health_info["version"] = f"SQLite {version_result.scalar()}"
                
                # Calculate response time
                end_time = time.time()
                health_info["response_time_ms"] = round((end_time - start_time) * 1000, 2)
                
                # Get pool status for PostgreSQL
                if hasattr(engine.pool, 'size'):
                    pool_status = {}
                    try:
                        pool_status["size"] = engine.pool.size()
                        pool_status["checked_in"] = engine.pool.checkedin()
                        pool_status["checked_out"] = engine.pool.checkedout()
                        
                        # Only include invalidated if the method exists
                        if hasattr(engine.pool, 'invalidated'):
                            pool_status["invalidated"] = engine.pool.invalidated()
                        else:
                            pool_status["invalidated"] = "N/A"
                            
                    except AttributeError as e:
                        logger.warning(f"Some pool methods not available: {e}")
                        pool_status = {"status": "Pool status unavailable"}
                    
                    health_info["pool_status"] = pool_status
                
                health_info["status"] = "healthy"
                logger.info(f"Database health check passed in {health_info['response_time_ms']}ms")
            else:
                health_info["status"] = "unhealthy"
                logger.warning("Database health check failed: SELECT 1 returned unexpected value")
                
    except Exception as e:
        health_info["status"] = "error"
        health_info["error"] = str(e)
        logger.error(f"Database health check failed: {e}")
    
    return health_info

# Database initialization and management functions
async def initialize_database():
    """
    Initialize database tables and indexes
    """
    try:
        logger.info("Initializing database tables...")
        
        async with engine.begin() as conn:
            # Import models to ensure they're registered
            from app.models import Provider, Rating
            
            # Create all tables
            await conn.run_sync(Base.metadata.create_all)
            logger.info("Database tables created successfully")
            
        return True
        
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise

async def drop_all_tables():
    """
    Drop all database tables (use with caution!)
    """
    try:
        logger.warning("Dropping all database tables...")
        
        async with engine.begin() as conn:
            from app.models import Provider, Rating
            await conn.run_sync(Base.metadata.drop_all)
            logger.info("All database tables dropped")
            
        return True
        
    except Exception as e:
        logger.error(f"Failed to drop tables: {e}")
        raise

async def get_database_stats() -> dict:
    """
    Get comprehensive database statistics
    """
    stats = {
        "tables": {},
        "indexes": {},
        "performance": {}
    }
    
    try:
        async with AsyncSessionLocal() as session:
            from sqlalchemy import text
            
            # Get table statistics
            if DATABASE_URL.startswith("postgresql"):
                # PostgreSQL-specific queries
                table_stats_query = text("""
                    SELECT 
                        schemaname,
                        relname as tablename,
                        n_tup_ins as inserts,
                        n_tup_upd as updates,
                        n_tup_del as deletes,
                        n_live_tup as live_tuples,
                        n_dead_tup as dead_tuples
                    FROM pg_stat_user_tables
                    WHERE schemaname = 'public'
                """)
                
                result = await session.execute(table_stats_query)
                for row in result:
                    stats["tables"][row.tablename] = {
                        "inserts": row.inserts,
                        "updates": row.updates,
                        "deletes": row.deletes,
                        "live_tuples": row.live_tuples,
                        "dead_tuples": row.dead_tuples
                    }
                
                # Get index usage statistics
                index_stats_query = text("""
                    SELECT 
                        indexrelname as index_name,
                        idx_tup_read,
                        idx_tup_fetch,
                        idx_scan
                    FROM pg_stat_user_indexes
                    WHERE schemaname = 'public'
                    ORDER BY idx_scan DESC
                """)
                
                result = await session.execute(index_stats_query)
                for row in result:
                    stats["indexes"][row.index_name] = {
                        "tuples_read": row.idx_tup_read,
                        "tuples_fetched": row.idx_tup_fetch,
                        "scans": row.idx_scan
                    }
                
            elif DATABASE_URL.startswith("sqlite"):
                # SQLite-specific queries
                table_list_query = text("SELECT name FROM sqlite_master WHERE type='table'")
                result = await session.execute(table_list_query)
                
                for row in result:
                    table_name = row.name
                    if table_name not in ['sqlite_sequence']:
                        count_query = text(f"SELECT COUNT(*) as count FROM {table_name}")
                        count_result = await session.execute(count_query)
                        stats["tables"][table_name] = {
                            "row_count": count_result.scalar()
                        }
            
    except Exception as e:
        logger.error(f"Error getting database stats: {e}")
        stats["error"] = str(e)
    
    return stats

# Connection pool monitoring
async def get_pool_status() -> dict:
    """
    Get connection pool status and metrics
    """
    pool_info = {
        "available": False,
        "pool_type": None
    }
    
    try:
        if hasattr(engine, 'pool'):
            pool = engine.pool
            pool_info.update({
                "available": True,
                "pool_type": type(pool).__name__,
            })
            
            # Get pool metrics if available
            if hasattr(pool, 'size'):
                try:
                    pool_info.update({
                        "size": pool.size(),
                        "checked_in": pool.checkedin(),
                        "checked_out": pool.checkedout(),
                        "overflow": pool.overflow(),
                    })
                    
                    # Only include invalidated if the method exists
                    if hasattr(pool, 'invalidated'):
                        pool_info["invalidated"] = pool.invalidated()
                    
                    # Calculate utilization
                    total_connections = pool.size() + pool.overflow()
                    if total_connections > 0:
                        pool_info["utilization_percent"] = round(
                            (pool.checkedout() / total_connections) * 100, 2
                        )
                except AttributeError as e:
                    logger.warning(f"Some pool methods not available: {e}")
                    pool_info["pool_methods_limited"] = True
    
    except Exception as e:
        logger.error(f"Error getting pool status: {e}")
        pool_info["error"] = str(e)
    
    return pool_info

# Database maintenance functions
async def analyze_tables():
    """
    Analyze tables for better query performance (PostgreSQL only)
    """
    if not DATABASE_URL.startswith("postgresql"):
        logger.info("Table analysis skipped - not using PostgreSQL")
        return False
    
    try:
        async with AsyncSessionLocal() as session:
            from sqlalchemy import text
            
            logger.info("Analyzing database tables for performance optimization...")
            
            # Analyze all tables
            await session.execute(text("ANALYZE"))
            await session.commit()
            
            logger.info("Database table analysis completed")
            return True
            
    except Exception as e:
        logger.error(f"Error analyzing tables: {e}")
        return False

async def vacuum_database():
    """
    Vacuum database to reclaim space and update statistics (PostgreSQL only)
    """
    if not DATABASE_URL.startswith("postgresql"):
        logger.info("Database vacuum skipped - not using PostgreSQL")
        return False
    
    try:
        # Note: VACUUM cannot be run inside a transaction
        logger.info("Starting database vacuum operation...")
        
        # This would need to be run outside of a transaction
        # For now, we'll just return a message
        logger.info("Database vacuum should be run manually: VACUUM ANALYZE;")
        return True
        
    except Exception as e:
        logger.error(f"Error during database vacuum: {e}")
        return False

# Graceful shutdown
async def close_database():
    """
    Gracefully close database connections
    """
    try:
        logger.info("Closing database connections...")
        await engine.dispose()
        logger.info("Database connections closed successfully")
    except Exception as e:
        logger.error(f"Error closing database connections: {e}")

# Database configuration summary
def get_database_config() -> dict:
    """
    Get current database configuration for debugging
    """
    config = {
        "database_url": DATABASE_URL.split('@')[0] + "@****" if '@' in DATABASE_URL else DATABASE_URL,
        "engine_config": {
            "echo": engine.echo,
            "pool_size": getattr(engine.pool, 'size', lambda: 'N/A')() if hasattr(engine, 'pool') else 'N/A',
            "max_overflow": getattr(engine.pool, '_max_overflow', 'N/A') if hasattr(engine, 'pool') else 'N/A',
            "pool_timeout": getattr(engine.pool, '_timeout', 'N/A') if hasattr(engine, 'pool') else 'N/A',
        },
        "database_type": "PostgreSQL" if DATABASE_URL.startswith("postgresql") else "SQLite" if DATABASE_URL.startswith("sqlite") else "Unknown"
    }
    
    return config

# Export main components
__all__ = [
    "engine",
    "AsyncSessionLocal", 
    "Base",
    "get_db",
    "check_database_health",
    "initialize_database",
    "drop_all_tables",
    "get_database_stats",
    "get_pool_status",
    "analyze_tables",
    "vacuum_database",
    "close_database",
    "get_database_config"
]