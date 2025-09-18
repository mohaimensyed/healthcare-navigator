# Healthcare Cost Navigator

A FastAPI-based web service that enables patients to search for hospitals offering MS-DRG procedures, view estimated prices & quality ratings, and interact with an AI assistant for natural language queries.

## Features

- **Provider Search**: Search hospitals by DRG code/description, location, and radius
- **AI Assistant**: Natural language interface powered by OpenAI GPT-4o-mini
- **Quality Ratings**: Mock star ratings (1-10) for provider quality assessment
- **Geographic Search**: Radius-based search using ZIP codes
- **Cost Analysis**: Sort and compare hospital costs for procedures
- **RESTful API**: Clean, documented API endpoints
- **Docker Support**: Complete containerized deployment

## Tech Stack

- **Backend**: Python 3.11, FastAPI, async SQLAlchemy
- **Database**: PostgreSQL with asyncpg driver
- **AI**: OpenAI GPT-4o-mini for natural language processing
- **Data Processing**: pandas, geopy for geocoding
- **Containerization**: Docker & Docker Compose

## Quick Start

### Prerequisites

- Docker and Docker Compose installed
- OpenAI API key
- Sample healthcare data (CMS format CSV)

### 1. Clone and Setup

```bash
git clone <repository-url>
cd healthcare-cost-navigator
```

### 2. Environment Configuration

Create a `.env` file in the root directory:

```env
# Database Configuration
DATABASE_URL=postgresql+asyncpg://postgres:postgres@db:5432/healthcare

# OpenAI Configuration (required)
OPENAI_API_KEY=your_openai_api_key_here

# Optional settings
POSTGRES_DB=healthcare
POSTGRES_USER=postgres  
POSTGRES_PASSWORD=postgres
```

### 3. Prepare Sample Data

Place your CMS sample data in the `data/` directory:

```bash
mkdir -p data
# Place your sample_prices_ny.csv file in data/
```

### 4. Start Services

```bash
# Start database and application
docker-compose up -d

# Check if services are running
docker-compose ps
```

### 5. Initialize Database and Load Data

```bash
# Initialize database tables
docker-compose exec app python app/init_db.py init

# Run the ETL process to load data
docker-compose exec app python etl.py
```

### 6. Test the Application

Visit http://localhost:8000 for the web interface, or test the API:

```bash
# Health check
curl http://localhost:8000/health

# Search providers
curl "http://localhost:8000/providers?drg=470&zip=10001&radius_km=50"

# Ask AI assistant
curl -X POST http://localhost:8000/ask \
  -H "Content-Type: application/json" \
  -d '{"question": "Who is cheapest for knee replacement near 10001?"}'
```

## API Documentation

### GET /providers

Search hospitals by DRG, location, and radius.

**Parameters:**
- `drg` (required): MS-DRG code (e.g., "470") or description (e.g., "knee replacement")
- `zip` (required): ZIP code for search center
- `radius_km` (optional): Search radius in kilometers (default: 50)
- `limit` (optional): Maximum results (default: 50)

**Example:**
```bash
curl "http://localhost:8000/providers?drg=470&zip=10001&radius_km=25&limit=10"
```

**Response:**
```json
[
  {
    "provider_id": "330123",
    "provider_name": "MOUNT SINAI HOSPITAL",
    "provider_city": "NEW YORK", 
    "provider_state": "NY",
    "provider_zip_code": "10029",
    "ms_drg_definition": "470 - Major Joint Replacement w/o MCC",
    "total_discharges": 245,
    "average_covered_charges": 84621.50,
    "average_total_payments": 21515.75,
    "average_medicare_payments": 19024.25,
    "average_rating": 8.5,
    "distance_km": 12.3
  }
]
```

### POST /ask

Natural language interface for healthcare queries.

**Request Body:**
```json
{
  "question": "Who has the best ratings for heart surgery near 10032?"
}
```

**Response:**
```json
{
  "answer": "Based on the data, Mount Sinai Hospital (rating: 9.0/10) and NYU Langone (rating: 8.7/10) have the highest ratings for cardiac procedures near 10032.",
  "sql_query": "SELECT p.provider_name, AVG(r.rating) as avg_rating...",
  "data_used": [...]
}
```

### Additional Endpoints

- `GET /health` - Health check with database connectivity
- `GET /stats` - Database statistics
- `GET /examples` - Example AI prompts
- `GET /providers/top-rated` - Top-rated providers
- `GET /providers/cheapest` - Most affordable providers

## AI Assistant Examples

The AI assistant can handle these types of queries:

### Cost-Related Queries
- "Who is the cheapest for DRG 470 within 25 miles of 10001?"
- "What's the average cost for knee replacement in NYC?" 
- "Show me the most affordable cardiac procedures"
- "Compare costs for hip surgery between hospitals"

### Quality-Related Queries  
- "Which hospitals have the best ratings for heart surgery?"
- "Find top-rated providers for emergency care"
- "Show me hospitals with ratings above 8.5"
- "What are the best hospitals for orthopedic surgery?"

### Location-Based Queries
- "Find hospitals near ZIP code 10032 with good ratings"
- "Show me cardiac providers within 20 miles of Manhattan"
- "What are my options for surgery in Albany area?"

### Comparative Queries
- "Which hospital offers the best value for knee replacement?"
- "Compare Mount Sinai vs NYU for cardiac procedures"
- "Show me cost vs quality analysis for DRG 470"

## Development Setup

### Local Development

```bash
# Create virtual environment
python3.11 -m venv venv
source venv/bin/activate  # Linux/Mac
# or
venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt

# Set up environment
cp .env.example .env
# Edit .env with your settings

# Start PostgreSQL locally or use Docker
docker run --name postgres-dev \
  -e POSTGRES_DB=healthcare \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  -d postgres:15

# Initialize database
python app/init_db.py init

# Run ETL
python etl.py

# Start application with auto-reload
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### Project Structure

```
healthcare-cost-navigator/
├── app/
│   ├── __init__.py
│   ├── main.py              # FastAPI application
│   ├── database.py          # Database configuration
│   ├── models.py            # SQLAlchemy models
│   ├── schemas.py           # Pydantic schemas
│   ├── init_db.py          # Database initialization
│   └── services/
│       ├── __init__.py
│       ├── provider_service.py  # Provider business logic
│       └── ai_service.py        # AI assistant logic
├── data/
│   └── sample_prices_ny.csv    # CMS data file
├── etl.py                      # ETL script
├── requirements.txt            # Python dependencies
├── Dockerfile                  # Docker image definition
├── docker-compose.yml         # Multi-container setup
├── .env.example               # Environment template
├── .gitignore                 # Git ignore rules
└── README.md                  # This file
```

### Running Tests

```bash
# Install test dependencies
pip install pytest pytest-asyncio pytest-cov

# Run tests
pytest

# With coverage
pytest --cov=app tests/ --cov-report=html
```

## Data Format

The application expects CMS Medicare Provider data with these columns:

| Column | Example | Description |
|--------|---------|-------------|
| Provider Id | 330123 | CMS ID for the hospital |
| Provider Name | CLEVELAND CLINIC | Hospital name |
| Provider City | NEW YORK | Hospital city |
| Provider State | NY | Hospital state |
| Provider Zip Code | 10032 | Hospital ZIP code |
| DRG Definition | 470 - Major Joint Replacement w/o MCC | Procedure group |
| Total Discharges | 1539 | Volume indicator |
| Average Covered Charges | 84621 | Avg. hospital bill |
| Average Total Payments | 21515 | Total paid amount |
| Average Medicare Payments | 19024 | Medicare portion |

## Architecture

### Database Design

**Providers Table:**
- Stores hospital information and procedure data
- Geographic indexes for efficient radius queries
- Cost-based indexes for sorting
- Composite unique constraint on provider_id + DRG

**Ratings Table:**
- Mock quality ratings (1-10 scale)
- Foreign key relationship with providers
- Categories: overall, cardiac, orthopedic, etc.

### AI Integration

**Two-Stage Process:**
1. **Query Generation**: OpenAI converts natural language to SQL
2. **Answer Generation**: OpenAI formats database results into natural language

**Safety Features:**
- SQL injection prevention
- Query validation (SELECT-only)
- Scope validation (healthcare-related only)
- Error handling and graceful fallbacks

### Geographic Search

**Coordinate Resolution:**
1. Hardcoded coordinates for common NYC ZIP codes
2. Database lookup from existing providers
3. Regional approximations based on ZIP patterns
4. Fallback coordinates

## Deployment

### Docker Production Deployment

```bash
# Production setup
docker-compose --profile production up -d

# With caching and admin interface
docker-compose --profile production --profile cache --profile admin up -d

# Check services
docker-compose ps
docker-compose logs -f app
```

### Environment Variables

**Required:**
- `OPENAI_API_KEY` - Your OpenAI API key

**Database:**
- `DATABASE_URL` - PostgreSQL connection string
- `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD` - Database credentials

**Optional:**
- `ENVIRONMENT` - deployment environment (development/production)
- `DEBUG` - enable debug mode
- `LOG_LEVEL` - logging level
- `CORS_ORIGINS` - allowed CORS origins

### Performance Tuning

**Database:**
- Connection pooling configured
- Proper indexing for search queries
- Batch processing for ETL

**API:**
- Async FastAPI for high concurrency
- Efficient SQLAlchemy queries
- Geographic calculation optimization

## Troubleshooting

### Common Issues

**Database Connection Errors:**
```bash
# Check if PostgreSQL is running
docker-compose ps db

# View database logs
docker-compose logs db

# Reset database
docker-compose down -v
docker-compose up -d
```

**ETL Failures:**
```bash
# Check data file format
head -n 5 data/sample_prices_ny.csv

# Run ETL with verbose output
docker-compose exec app python etl.py

# Check database after ETL
docker-compose exec app python app/init_db.py info
```

**OpenAI API Issues:**
- Verify API key in .env file
- Check OpenAI account credits/billing
- Test with simple query first

**No Search Results:**
- Verify data was loaded successfully
- Try broader search criteria (larger radius)
- Check DRG code format

### Logging

Enable detailed logging:
```bash
# Set environment variables
export LOG_LEVEL=DEBUG

# View application logs
docker-compose logs -f app

# View specific service logs
docker-compose logs -f db
```

### Database Management

```bash
# Connect to database
docker-compose exec db psql -U postgres -d healthcare

# Check data
SELECT COUNT(*) FROM providers;
SELECT COUNT(*) FROM ratings;

# Reset database (WARNING: deletes all data)
docker-compose exec app python app/init_db.py reset
```

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature-name`
3. Make changes and add tests
4. Run tests: `pytest`
5. Submit a pull request

## License

MIT License - see LICENSE file for details.

## Support

For issues and questions:
1. Check the troubleshooting section above
2. Review the Docker logs: `docker-compose logs -f`
3. Ensure your .env file has the correct values
4. Verify your OpenAI API key is valid

## Next Steps & Improvements

### Immediate Enhancements
- Add real Medicare star ratings data
- Implement Redis caching for API responses
- Add user authentication and saved searches
- Create interactive web frontend

### Advanced Features
- Multi-state data support
- Real-time price updates
- Provider comparison tools
- Insurance coverage estimation
- Mobile API and app support

### Data Quality
- Integrate with real-time CMS data feeds
- Add data validation and quality metrics
- Provider verification and contact information
- Historical price trend analysis