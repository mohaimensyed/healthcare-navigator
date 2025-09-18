from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.responses import HTMLResponse, PlainTextResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from typing import List
import logging
import os
from dotenv import load_dotenv

from app.database import get_db
from app.models import Provider
from app.schemas import ProviderResponse, AskRequest, AskResponse
from app.services.provider_service import ProviderService
from app.services.ai_service import AIService

load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Healthcare Cost Navigator",
    description="Search for hospitals by MS-DRG procedures and get AI-powered assistance",
    version="1.0.0"
)

# Initialize services
try:
    provider_service = ProviderService()
    ai_service = AIService()
    logger.info("Services initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize services: {e}")
    raise

@app.get("/", response_class=HTMLResponse)
async def root():
    """Simple HTML interface for testing"""
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Healthcare Cost Navigator</title>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <style>
            body { font-family: Arial, sans-serif; max-width: 1200px; margin: 0 auto; padding: 20px; }
            .form-group { margin-bottom: 15px; }
            label { display: block; margin-bottom: 5px; font-weight: bold; }
            input, textarea, select { width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px; }
            button { background-color: #007bff; color: white; padding: 10px 20px; border: none; border-radius: 4px; cursor: pointer; }
            button:hover { background-color: #0056b3; }
            .results { margin-top: 20px; padding: 15px; background-color: #f8f9fa; border-radius: 4px; white-space: pre-wrap; }
            .answer { margin-top: 20px; padding: 15px; background-color: #e8f5e8; border-radius: 4px; font-size: 16px; line-height: 1.5; }
            .error { color: #dc3545; background-color: #f8d7da; padding: 10px; border-radius: 4px; }
            .examples { margin-top: 20px; font-size: 0.9em; color: #666; }
        </style>
    </head>
    <body>
        <h1>Healthcare Cost Navigator</h1>
        <p>Search for hospitals by procedure and location, or ask questions in natural language.</p>
        
        <h2>🏥 Search Providers</h2>
        <form id="searchForm">
            <div class="form-group">
                <label>DRG Code or Description:</label>
                <input type="text" id="drg" name="drg" placeholder="470 or 'knee replacement'" required>
            </div>
            <div class="form-group">
                <label>ZIP Code:</label>
                <input type="text" id="zip" name="zip" placeholder="10001" required>
            </div>
            <div class="form-group">
                <label>Search Radius (km):</label>
                <input type="number" id="radius" name="radius" value="50" min="1" max="500">
            </div>
            <div class="form-group">
                <label>Max Results:</label>
                <input type="number" id="limit" name="limit" value="20" min="1" max="100">
            </div>
            <button type="submit">Search Hospitals</button>
        </form>
        
        <h2>🤖 AI Assistant</h2>
        <form id="askForm">
            <div class="form-group">
                <label>Ask a question about healthcare costs and quality:</label>
                <textarea id="question" name="question" rows="3" 
                         placeholder="Who is the cheapest for knee replacement near 10001?"></textarea>
            </div>
            <button type="submit">Ask AI Assistant</button>
        </form>
        
        <div class="examples">
            <strong>Example questions:</strong><br>
            • "Who is the cheapest for DRG 470 within 25 miles of 10001?"<br>
            • "What are the best rated hospitals for heart surgery in New York?"<br>
            • "Show me hospitals with lowest cost for knee replacement"<br>
            • "Which providers have the highest ratings for cardiac procedures?"<br>
            • "Find hospitals near ZIP code 10032 with good ratings"
        </div>
        
        <div id="results"></div>
        
        <script>
            function showError(message) {
                document.getElementById('results').innerHTML = 
                    '<div class="error">Error: ' + message + '</div>';
            }
            
            function showResults(data) {
                if (Array.isArray(data) && data.length > 0) {
                    let html = '<div class="results"><h3>Found ' + data.length + ' hospitals:</h3>';
                    data.slice(0, 10).forEach(function(provider, index) {
                        html += '<div style="margin-bottom: 15px; padding: 10px; border-left: 3px solid #007bff; background-color: #f8f9fa;">';
                        html += '<strong>' + (index + 1) + '. ' + provider.provider_name + '</strong><br>';
                        html += 'Location: ' + provider.provider_city + ', ' + provider.provider_state + ' ' + provider.provider_zip_code;
                        if (provider.distance_km) html += ' (' + provider.distance_km + ' km away)';
                        html += '<br>';
                        html += 'Procedure: ' + provider.ms_drg_definition + '<br>';
                        html += 'Cost: ' + provider.average_covered_charges.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2}) + '<br>';
                        if (provider.average_rating) {
                            html += 'Rating: ' + provider.average_rating.toFixed(1) + '/10<br>';
                        }
                        html += 'Discharges: ' + provider.total_discharges;
                        html += '</div>';
                    });
                    html += '</div>';
                    document.getElementById('results').innerHTML = html;
                } else {
                    document.getElementById('results').innerHTML = 
                        '<div class="results">No results found. Try a different search or larger radius.</div>';
                }
            }
            
            function showAnswer(answer) {
                document.getElementById('results').innerHTML = 
                    '<div class="answer">' + answer + '</div>';
            }
            
            document.getElementById('searchForm').addEventListener('submit', async (e) => {
                e.preventDefault();
                const formData = new FormData(e.target);
                const params = new URLSearchParams();
                for (let [key, value] of formData.entries()) {
                    if (value) params.append(key, value);
                }
                
                try {
                    const response = await fetch('/providers?' + params);
                    if (!response.ok) {
                        const error = await response.json();
                        throw new Error(error.detail || 'Search failed');
                    }
                    const data = await response.json();
                    showResults(data);
                } catch (error) {
                    showError(error.message);
                }
            });
            
            document.getElementById('askForm').addEventListener('submit', async (e) => {
                e.preventDefault();
                const question = document.getElementById('question').value.trim();
                if (!question) {
                    showError('Please enter a question');
                    return;
                }
                
                try {
                    const response = await fetch('/ask', {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify({question: question})
                    });
                    
                    if (!response.ok) {
                        const error = await response.json();
                        throw new Error(error.detail || 'AI request failed');
                    }
                    
                    const answer = await response.text();
                    showAnswer(answer);
                } catch (error) {
                    showError(error.message);
                }
            });
        </script>
    </body>
    </html>
    """
    return html_content


@app.get("/providers", response_model=List[ProviderResponse])
async def search_providers(
    drg: str = Query(..., description="MS-DRG code or description", example="470"),
    zip_code: str = Query(..., description="ZIP code for search center", alias="zip", example="10001"),
    radius_km: int = Query(50, description="Search radius in kilometers", ge=1, le=500),
    limit: int = Query(50, description="Maximum number of results", ge=1, le=100),
    db: AsyncSession = Depends(get_db)
):
    """Search for healthcare providers by DRG and location"""
    try:
        if not drg.strip():
            raise HTTPException(status_code=400, detail="DRG parameter cannot be empty")
        if not zip_code.strip():
            raise HTTPException(status_code=400, detail="ZIP code parameter cannot be empty")
        if not zip_code.replace('-', '').isdigit() or len(zip_code.split('-')[0]) != 5:
            raise HTTPException(status_code=400, detail="Invalid ZIP code format")
        
        results = await provider_service.search_providers(db, drg.strip(), zip_code, radius_km, limit)
        return results
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in search_providers: {e}")
        raise HTTPException(status_code=500, detail="Internal server error during provider search")


@app.post("/ask", response_class=PlainTextResponse)
async def ask_ai_assistant(request: AskRequest, db: AsyncSession = Depends(get_db)):
    try:
        if not request.question.strip():
            raise HTTPException(status_code=400, detail="Question cannot be empty")
        if len(request.question) > 1000:
            raise HTTPException(status_code=400, detail="Question too long (max 1000 characters)")
        
        response = await ai_service.process_question(db, request.question.strip())
        return response.answer
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in ask_ai_assistant: {e}")
        return "I encountered an error processing your question. Please try again."


@app.post("/ask-json", response_model=AskResponse)
async def ask_ai_assistant_json(request: AskRequest, db: AsyncSession = Depends(get_db)):
    try:
        if not request.question.strip():
            raise HTTPException(status_code=400, detail="Question cannot be empty")
        if len(request.question) > 1000:
            raise HTTPException(status_code=400, detail="Question too long (max 1000 characters)")
        
        response = await ai_service.process_question(db, request.question.strip())
        return response
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in ask_ai_assistant_json: {e}")
        raise HTTPException(status_code=500, detail="Internal server error during AI processing")


@app.get("/health")
async def health_check(db: AsyncSession = Depends(get_db)):
    try:
        result = await db.execute(select(func.count(Provider.id)))
        provider_count = result.scalar()
        return {
            "status": "healthy",
            "database": "connected",
            "providers_in_db": provider_count,
            "version": "1.0.0"
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail="Service unhealthy")


@app.get("/stats")
async def get_statistics(db: AsyncSession = Depends(get_db)):
    try:
        stats = await provider_service.get_provider_statistics(db)
        return stats
    except Exception as e:
        logger.error(f"Error getting statistics: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving statistics")


@app.get("/examples")
async def get_example_prompts():
    try:
        examples = ai_service.get_example_prompts()
        return {"examples": examples}
    except Exception as e:
        logger.error(f"Error getting examples: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving examples")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
