# tests/test_api.py
import pytest
import asyncio
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
import os

from app.main import app
from app.database import get_db
from app.models import Base

# Test database URL
TEST_DATABASE_URL = "postgresql+asyncpg://user:password@localhost:5432/healthcare_nav_test"

@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture(scope="session")
async def test_engine():
    """Create test database engine"""
    engine = create_async_engine(TEST_DATABASE_URL, echo=False)
    
    # Create tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    yield engine
    
    # Cleanup
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    
    await engine.dispose()

@pytest.fixture
async def test_db_session(test_engine):
    """Create test database session"""
    TestSessionLocal = sessionmaker(test_engine, class_=AsyncSession, expire_on_commit=False)
    
    async with TestSessionLocal() as session:
        yield session

@pytest.fixture
async def client(test_db_session):
    """Create test client with test database"""
    
    async def override_get_db():
        yield test_db_session
    
    app.dependency_overrides[get_db] = override_get_db
    
    async with AsyncClient(app=app, base_url="http://test") as ac:
        yield ac
    
    app.dependency_overrides.clear()

@pytest.mark.asyncio
async def test_health_check(client):
    """Test health check endpoint"""
    response = await client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"

@pytest.mark.asyncio
async def test_providers_search_missing_params(client):
    """Test providers endpoint with missing parameters"""
    response = await client.get("/providers")
    assert response.status_code == 422  # Validation error

@pytest.mark.asyncio
async def test_providers_search_with_params(client):
    """Test providers endpoint with valid parameters"""
    response = await client.get("/providers?drg=470&zip=10001&radius_km=50")
    assert response.status_code == 200
    assert isinstance(response.json(), list)

@pytest.mark.asyncio 
async def test_ask_endpoint_out_of_scope(client):
    """Test AI assistant with out-of-scope question"""
    response = await client.post(
        "/ask", 
        json={"question": "What's the weather today?"}
    )
    assert response.status_code == 200
    data = response.json()
    assert "hospital pricing and quality information" in data["answer"]

@pytest.mark.asyncio
async def test_ask_endpoint_healthcare_question(client):
    """Test AI assistant with healthcare question"""
    response = await client.post(
        "/ask",
        json={"question": "Find cheapest hospital for knee replacement"}
    )
    assert response.status_code == 200
    data = response.json()
    assert "answer" in data