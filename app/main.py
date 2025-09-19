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
    description="Search for hospitals by MS-DRG procedures and get AI-powered assistance with enhanced ranking",
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
    """Enhanced HTML interface with better styling and value-based ranking display"""
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Healthcare Cost Navigator</title>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <style>
            body { 
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
                max-width: 1200px; 
                margin: 0 auto; 
                padding: 20px; 
                background-color: #f8f9fa;
                line-height: 1.6;
            }
            .header {
                background: linear-gradient(135deg, #007bff 0%, #0056b3 100%);
                color: white;
                padding: 30px;
                border-radius: 10px;
                margin-bottom: 30px;
                text-align: center;
            }
            .container {
                background: white;
                padding: 30px;
                border-radius: 10px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                margin-bottom: 20px;
            }
            .form-group { 
                margin-bottom: 20px; 
            }
            label { 
                display: block; 
                margin-bottom: 8px; 
                font-weight: 600; 
                color: #333;
            }
            input, textarea, select { 
                width: 100%; 
                padding: 12px; 
                border: 2px solid #e9ecef; 
                border-radius: 6px; 
                font-size: 14px;
                transition: border-color 0.3s;
            }
            input:focus, textarea:focus { 
                border-color: #007bff; 
                outline: none; 
                box-shadow: 0 0 0 3px rgba(0,123,255,0.1);
            }
            button { 
                background: linear-gradient(135deg, #28a745 0%, #20c997 100%);
                color: white; 
                padding: 12px 24px; 
                border: none; 
                border-radius: 6px; 
                cursor: pointer; 
                font-size: 16px;
                font-weight: 600;
                transition: transform 0.2s;
            }
            button:hover { 
                transform: translateY(-2px);
                box-shadow: 0 4px 12px rgba(0,0,0,0.15);
            }
            .results { 
                margin-top: 20px; 
                padding: 20px; 
                background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%); 
                border-radius: 8px; 
                border-left: 4px solid #007bff;
            }
            .provider-card { 
                margin-bottom: 20px; 
                padding: 20px; 
                background: white;
                border-radius: 8px; 
                box-shadow: 0 2px 8px rgba(0,0,0,0.1);
                border-left: 4px solid #007bff;
                transition: transform 0.2s;
            }
            .provider-card:hover {
                transform: translateY(-2px);
                box-shadow: 0 4px 15px rgba(0,0,0,0.15);
            }
            .provider-header {
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 12px;
            }
            .provider-name {
                font-size: 18px;
                font-weight: 700;
                color: #007bff;
                margin: 0;
            }
            .rating-badge {
                background: linear-gradient(135deg, #28a745 0%, #20c997 100%);
                color: white;
                padding: 6px 12px;
                border-radius: 20px;
                font-size: 13px;
                font-weight: 600;
            }
            .provider-details {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                gap: 15px;
                margin-top: 15px;
            }
            .detail-item {
                display: flex;
                align-items: center;
                gap: 8px;
                font-size: 14px;
            }
            .icon {
                font-size: 16px;
                width: 20px;
            }
            .cost-highlight {
                color: #dc3545;
                font-weight: 700;
                font-size: 16px;
            }
            .answer { 
                margin-top: 20px; 
                padding: 20px; 
                background: linear-gradient(135deg, #e8f5e8 0%, #d4edda 100%); 
                border-radius: 8px; 
                font-size: 16px; 
                line-height: 1.6;
                border-left: 4px solid #28a745;
            }
            .error { 
                color: #dc3545; 
                background: linear-gradient(135deg, #f8d7da 0%, #f5c6cb 100%); 
                padding: 15px; 
                border-radius: 6px; 
                border-left: 4px solid #dc3545;
            }
            .examples { 
                margin-top: 25px; 
                padding: 20px;
                background: #fff3cd;
                border-radius: 8px;
                border-left: 4px solid #ffc107;
            }
            .examples h4 {
                margin-top: 0;
                color: #856404;
            }
            .example-item {
                margin: 8px 0;
                color: #666;
                cursor: pointer;
                padding: 5px;
                border-radius: 4px;
                transition: background-color 0.2s;
            }
            .example-item:hover {
                background-color: #fff8e1;
            }
            .value-score {
                background: linear-gradient(135deg, #6f42c1 0%, #e83e8c 100%);
                color: white;
                padding: 4px 8px;
                border-radius: 12px;
                font-size: 12px;
                font-weight: 600;
            }
            .loading {
                display: none;
                text-align: center;
                padding: 20px;
                color: #666;
            }
            .spinner {
                border: 3px solid #f3f3f3;
                border-top: 3px solid #007bff;
                border-radius: 50%;
                width: 30px;
                height: 30px;
                animation: spin 1s linear infinite;
                margin: 0 auto 10px;
            }
            @keyframes spin {
                0% { transform: rotate(0deg); }
                100% { transform: rotate(360deg); }
            }
        </style>
    </head>
    <body>
        <div class="header">
            <h1>🏥 Healthcare Cost Navigator</h1>
            <p>Find the best value hospitals with smart ranking that balances cost, quality, and experience</p>
        </div>
        
        <div class="container">
            <h2>🔍 Search Providers</h2>
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
        </div>
        
        <div class="container">
            <h2>🤖 AI Assistant</h2>
            <form id="askForm">
                <div class="form-group">
                    <label>Ask a question about healthcare costs and quality:</label>
                    <textarea id="question" name="question" rows="3" 
                             placeholder="Who has the best value for knee replacement near 10001?"></textarea>
                </div>
                <button type="submit">Ask AI Assistant</button>
            </form>
        </div>
        
        <div class="examples">
            <h4>💡 Example Questions (click to try):</h4>
            <div class="example-item" onclick="fillQuestion('Who is the cheapest for DRG 470 within 25 miles of 10001?')">
                • "Who is the cheapest for DRG 470 within 25 miles of 10001?"
            </div>
            <div class="example-item" onclick="fillQuestion('What are the best rated hospitals for heart surgery in New York?')">
                • "What are the best rated hospitals for heart surgery in New York?"
            </div>
            <div class="example-item" onclick="fillQuestion('Show me the best value hospitals for knee replacement near Manhattan')">
                • "Show me the best value hospitals for knee replacement near Manhattan"
            </div>
            <div class="example-item" onclick="fillQuestion('Find cost-effective options for cardiac procedures with good ratings')">
                • "Find cost-effective options for cardiac procedures with good ratings"
            </div>
            <div class="example-item" onclick="fillQuestion('Which hospital offers the best combination of quality and affordability?')">
                • "Which hospital offers the best combination of quality and affordability?"
            </div>
        </div>
        
        <div class="loading" id="loading">
            <div class="spinner"></div>
            <p>Searching hospitals...</p>
        </div>
        
        <div id="results"></div>
        
        <script>
            function showLoading() {
                document.getElementById('loading').style.display = 'block';
                document.getElementById('results').innerHTML = '';
            }
            
            function hideLoading() {
                document.getElementById('loading').style.display = 'none';
            }
            
            function fillQuestion(question) {
                document.getElementById('question').value = question;
            }
            
            function showError(message) {
                hideLoading();
                document.getElementById('results').innerHTML = 
                    '<div class="error">❌ Error: ' + message + '</div>';
            }
            
            function calculateValueScore(provider) {
                try {
                    // Same composite scoring as backend
                    const cost = Math.max(provider.average_covered_charges || 50000, 1000);
                    const costScore = 1000000 / cost;
                    const ratingScore = (provider.average_rating || 5.0) * 15;
                    const distanceScore = Math.max(0, 100 - (provider.distance_km || 50) * 1.5);
                    const volumeScore = Math.min(Math.log((provider.total_discharges || 0) + 1) * 10, 50);
                    
                    return Math.round(costScore * 0.4 + ratingScore * 0.35 + distanceScore * 0.15 + volumeScore * 0.1);
                } catch {
                    return 0;
                }
            }
            
            function showResults(data) {
                hideLoading();
                if (Array.isArray(data) && data.length > 0) {
                    let html = '<div class="results"><h3>🏆 Found ' + data.length + ' hospitals (ranked by value score):</h3>';
                    
                    data.slice(0, 10).forEach(function(provider, index) {
                        const valueScore = calculateValueScore(provider);
                        
                        html += '<div class="provider-card">';
                        html += '<div class="provider-header">';
                        html += '<h3 class="provider-name">' + (index + 1) + '. ' + provider.provider_name + '</h3>';
                        html += '<div>';
                        if (provider.average_rating) {
                            html += '<span class="rating-badge">⭐ ' + provider.average_rating.toFixed(1) + '/10</span> ';
                        }
                        html += '<span class="value-score">Value: ' + valueScore + '</span>';
                        html += '</div>';
                        html += '</div>';
                        
                        html += '<div class="provider-details">';
                        html += '<div class="detail-item"><span class="icon">📍</span>' + provider.provider_city + ', ' + provider.provider_state + ' ' + provider.provider_zip_code + '</div>';
                        if (provider.distance_km) {
                            html += '<div class="detail-item"><span class="icon">📏</span>' + provider.distance_km + ' km away</div>';
                        }
                        html += '<div class="detail-item"><span class="icon">🏥</span>' + provider.ms_drg_definition + '</div>';
                        html += '<div class="detail-item"><span class="icon cost-highlight">💰</span><span class="cost-highlight">$' + provider.average_covered_charges.toLocaleString('en-US', {maximumFractionDigits: 0}) + '</span></div>';
                        if (provider.total_discharges > 0) {
                            html += '<div class="detail-item"><span class="icon">📊</span>' + provider.total_discharges + ' procedures/year</div>';
                        }
                        html += '</div>';
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
                hideLoading();
                document.getElementById('results').innerHTML = 
                    '<div class="answer">🤖 ' + answer + '</div>';
            }
            
            document.getElementById('searchForm').addEventListener('submit', async (e) => {
                e.preventDefault();
                showLoading();
                
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
                showLoading();
                
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
    """
    Search for healthcare providers with enhanced multi-factor ranking
    Returns providers ranked by composite value score (cost + quality + distance + experience)
    """
    try:
        if not drg.strip():
            raise HTTPException(status_code=400, detail="DRG parameter cannot be empty")
        if not zip_code.strip():
            raise HTTPException(status_code=400, detail="ZIP code parameter cannot be empty")
        if not zip_code.replace('-', '').isdigit() or len(zip_code.split('-')[0]) != 5:
            raise HTTPException(status_code=400, detail="Invalid ZIP code format")
        
        logger.info(f"Enhanced provider search: DRG={drg}, ZIP={zip_code}, Radius={radius_km}km")
        
        results = await provider_service.search_providers(db, drg.strip(), zip_code, radius_km, limit)
        
        logger.info(f"Returning {len(results)} results with composite ranking")
        return results
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in search_providers: {e}")
        raise HTTPException(status_code=500, detail="Internal server error during provider search")


@app.post("/ask", response_class=PlainTextResponse)
async def ask_ai_assistant(request: AskRequest, db: AsyncSession = Depends(get_db)):
    """
    AI assistant with intent-aware ranking
    Automatically detects if user wants cheapest, best-rated, nearest, or best value
    """
    try:
        if not request.question.strip():
            raise HTTPException(status_code=400, detail="Question cannot be empty")
        if len(request.question) > 1000:
            raise HTTPException(status_code=400, detail="Question too long (max 1000 characters)")
        
        logger.info(f"AI assistant query: {request.question}")
        
        response = await ai_service.process_question(db, request.question.strip())
        
        logger.info(f"AI response generated successfully")
        return response.answer
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in ask_ai_assistant: {e}")
        return "I encountered an error processing your question. Please try again with a specific question about hospital costs or quality."


@app.post("/ask-json", response_model=AskResponse)
async def ask_ai_assistant_json(request: AskRequest, db: AsyncSession = Depends(get_db)):
    """
    AI assistant JSON response with enhanced debugging information
    """
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
    """Enhanced health check with database statistics"""
    try:
        # Check basic connectivity
        result = await db.execute(select(func.count(Provider.id)))
        provider_count = result.scalar()
        
        # Get basic stats
        stats = await provider_service.get_provider_statistics(db)
        
        return {
            "status": "healthy",
            "database": "connected",
            "providers_in_db": provider_count,
            "total_ratings": stats.get('total_ratings', 0),
            "average_rating": stats.get('average_rating', 0),
            "ranking_algorithm": "composite (cost 40% + rating 35% + distance 15% + volume 10%)",
            "version": "1.0.0"
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail="Service unhealthy")


@app.get("/stats")
async def get_statistics(db: AsyncSession = Depends(get_db)):
    """Get comprehensive database and ranking statistics"""
    try:
        stats = await provider_service.get_provider_statistics(db)
        
        # Add ranking algorithm info
        stats.update({
            "ranking_algorithm": {
                "type": "composite_scoring",
                "weights": {
                    "cost_effectiveness": 0.4,
                    "quality_rating": 0.35,
                    "distance_preference": 0.15,
                    "volume_experience": 0.1
                },
                "description": "Multi-factor ranking balancing cost, quality, proximity, and experience"
            },
            "search_features": {
                "enhanced_drg_matching": "Medical synonyms and fuzzy matching",
                "intent_detection": "Automatic optimization for cheapest/best-rated/nearest/value queries",
                "fallback_searches": "Broader geographic and procedure searches when no exact matches"
            }
        })
        
        return stats
    except Exception as e:
        logger.error(f"Error getting statistics: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving statistics")


@app.get("/examples")
async def get_example_prompts():
    """Get enhanced example prompts covering different query intents"""
    try:
        examples = ai_service.get_example_prompts()
        return {
            "examples": examples,
            "intents_supported": [
                "cheapest - focuses on cost optimization",
                "best_rated - prioritizes quality ratings", 
                "nearest - emphasizes proximity",
                "value - balances cost, quality, distance, and experience (default)"
            ],
            "ranking_explanation": "The system automatically detects your intent and optimizes results accordingly"
        }
    except Exception as e:
        logger.error(f"Error getting examples: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving examples")


# Additional endpoints for debugging and monitoring
@app.get("/top-rated")
async def get_top_rated_providers(
    drg: str = Query(None, description="Optional DRG filter"),
    limit: int = Query(10, description="Number of results", ge=1, le=50),
    db: AsyncSession = Depends(get_db)
):
    """Get top-rated providers (for comparison with composite ranking)"""
    try:
        results = await provider_service.get_top_rated_providers(db, drg, limit)
        return results
    except Exception as e:
        logger.error(f"Error getting top rated providers: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving top rated providers")


@app.get("/cheapest")
async def get_cheapest_providers(
    drg: str = Query(None, description="Optional DRG filter"),
    limit: int = Query(10, description="Number of results", ge=1, le=50),
    db: AsyncSession = Depends(get_db)
):
    """Get cheapest providers (for comparison with composite ranking)"""
    try:
        results = await provider_service.get_cheapest_providers(db, drg, limit)
        return results
    except Exception as e:
        logger.error(f"Error getting cheapest providers: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving cheapest providers")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")