from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.responses import HTMLResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, or_
from typing import List, Optional
import math
import os
from dotenv import load_dotenv

from app.database import get_db
from app.models import Provider, Rating
from app.schemas import ProviderResponse, ProviderSearchParams, AskRequest, AskResponse
from app.services.provider_service import ProviderService
from app.services.ai_service import AIService

load_dotenv()

app = FastAPI(
    title="Healthcare Cost Navigator",
    description="Search for hospitals by MS-DRG procedures and get AI-powered assistance",
    version="1.0.0"
)

# Initialize services
provider_service = ProviderService()
ai_service = AIService()

@app.get("/", response_class=HTMLResponse)
async def root():
    """Simple HTML interface for testing"""
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Healthcare Cost Navigator</title>
    </head>
    <body>
        <h1>Healthcare Cost Navigator</h1>
        <h2>Search Providers</h2>
        <form id="searchForm">
            <p>
                <label>DRG:</label>
                <input type="text" id="drg" name="drg" placeholder="470 or Major Joint Replacement" required>
            </p>
            <p>
                <label>ZIP Code:</label>
                <input type="text" id="zip" name="zip" placeholder="10001" required>
            </p>
            <p>
                <label>Radius (km):</label>
                <input type="number" id="radius" name="radius" value="50">
            </p>
            <p>
                <button type="submit">Search</button>
            </p>
        </form>
        
        <h2>AI Assistant</h2>
        <form id="askForm">
            <p>
                <textarea id="question" name="question" rows="3" cols="50" 
                         placeholder="Who is cheapest for knee replacement near 10001?"></textarea>
            </p>
            <p>
                <button type="submit">Ask</button>
            </p>
        </form>
        
        <div id="results"></div>
        
        <script>
            document.getElementById('searchForm').addEventListener('submit', async (e) => {
                e.preventDefault();
                const formData = new FormData(e.target);
                const params = new URLSearchParams();
                for (let [key, value] of formData.entries()) {
                    params.append(key, value);
                }
                
                try {
                    const response = await fetch('/providers?' + params);
                    const data = await response.json();
                    document.getElementById('results').innerHTML = '<pre>' + JSON.stringify(data, null, 2) + '</pre>';
                } catch (error) {
                    document.getElementById('results').innerHTML = 'Error: ' + error;
                }
            });
            
            document.getElementById('askForm').addEventListener('submit', async (e) => {
                e.preventDefault();
                const question = document.getElementById('question').value;
                
                try {
                    const response = await fetch('/ask', {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify({question: question})
                    });
                    const data = await response.json();
                    document.getElementById('results').innerHTML = '<pre>' + JSON.stringify(data, null, 2) + '</pre>';
                } catch (error) {
                    document.getElementById('results').innerHTML = 'Error: ' + error;
                }
            });
        </script>
    </body>
    </html>
    """
    return html_content

@app.get("/providers", response_model=List[ProviderResponse])
async def search_providers(
    drg: str = Query(..., description="MS-DRG code or description"),
    zip_code: str = Query(..., description="ZIP code for search center", alias="zip"),
    radius_km: int = Query(50, description="Search radius in kilometers"),
    limit: int = Query(50, description="Maximum number of results"),
    db: AsyncSession = Depends(get_db)
):
    """Search for healthcare providers by DRG and location"""
    try:
        results = await provider_service.search_providers(
            db, drg, zip_code, radius_km, limit
        )
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/ask", response_model=AskResponse)
async def ask_ai_assistant(
    request: AskRequest,
    db: AsyncSession = Depends(get_db)
):
    """Natural language interface for healthcare queries"""
    try:
        response = await ai_service.process_question(db, request.question)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "Healthcare Cost Navigator"}

# For development
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)