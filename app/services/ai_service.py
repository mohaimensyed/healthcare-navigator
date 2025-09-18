from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
import openai
import json
import os
from typing import Dict, List, Any, Optional
import re
import logging

from app.schemas import AskResponse

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AIService:
    def __init__(self):
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OPENAI_API_KEY environment variable is required")
        
        self.client = openai.AsyncOpenAI(api_key=api_key)
        
        # Common DRG mappings to help with queries
        self.drg_mappings = {
            'knee': ['470', '469', '468'],
            'hip': ['470', '469', '468'],
            'joint replacement': ['470', '469', '468'],
            'heart': ['246', '247', '248', '249', '250', '251', '252'],
            'cardiac': ['246', '247', '248', '249', '250', '251', '252'],
            'cardiovascular': ['246', '247', '248', '249', '250', '251', '252'],
            'bypass': ['231', '232', '233', '234', '235', '236'],
            'kidney': ['682', '683', '684', '685', '686', '687'],
            'dialysis': ['682', '683', '684', '685'],
            'emergency': ['981', '982', '983', '984'],
            'surgery': ['470', '480', '481', '482']
        }

        # NYC area ZIP code patterns for smarter location matching
        self.nyc_zip_patterns = {
            '100': 'Manhattan',
            '101': 'Manhattan', 
            '102': 'Manhattan',
            '112': 'Brooklyn',
            '113': 'Brooklyn',
            '104': 'Bronx',
            '114': 'Queens',
            '103': 'Staten Island'
        }
        
    async def process_question(self, db: AsyncSession, question: str) -> AskResponse:
        """Process natural language question and return grounded answer"""
        
        logger.info(f"Processing question: {question}")
        
        # Check if question is in scope
        if not self._is_healthcare_related(question):
            return AskResponse(
                answer="I can only help with hospital pricing and quality information. Please ask about medical procedures, costs, or hospital ratings. For example: 'What are the cheapest hospitals for knee replacement?' or 'Which hospitals have the best ratings for cardiac surgery?'",
                sql_query=None,
                data_used=None
            )
        
        try:
            # Generate SQL query from natural language
            sql_query = await self._generate_sql(question)
            
            if not sql_query:
                return AskResponse(
                    answer="I couldn't understand your question. Please try asking about specific procedures, costs, or hospital ratings. For example: 'What are the cheapest hospitals for knee replacement?' or 'Which hospitals have the best ratings for cardiac surgery?'",
                    sql_query=None,
                    data_used=None
                )
            
            logger.info(f"Generated SQL: {sql_query}")
            
            # Execute the SQL query safely
            try:
                result = await db.execute(text(sql_query))
                rows = result.fetchall()
                columns = list(result.keys()) if result.keys() else []
            except Exception as sql_error:
                logger.error(f"SQL execution error: {sql_error}")
                return AskResponse(
                    answer="I encountered an error with the database query. Please try rephrasing your question or ask about specific procedures like knee replacement or heart surgery.",
                    sql_query=sql_query,
                    data_used=None
                )
            
            # If no results, try fallback strategies
            if not rows:
                fallback_response = await self._try_fallback_searches(db, question)
                if fallback_response:
                    return fallback_response
                
                # Generate helpful "no results" message
                helpful_message = self._generate_helpful_no_results_message(question)
                return AskResponse(
                    answer=helpful_message,
                    sql_query=sql_query,
                    data_used=[]
                )
            
            # Convert results to dictionaries
            data_used = []
            for row in rows:
                row_dict = {}
                for i, column in enumerate(columns):
                    value = row[i] if i < len(row) else None
                    # Handle different data types safely
                    if isinstance(value, (int, float, str)):
                        row_dict[column] = value
                    else:
                        row_dict[column] = str(value) if value is not None else None
                data_used.append(row_dict)
            
            # Generate natural language answer
            answer = await self._generate_answer(question, data_used)
            
            return AskResponse(
                answer=answer,
                sql_query=sql_query,
                data_used=data_used[:10]  # Limit to first 10 results for response
            )
            
        except Exception as e:
            logger.error(f"Error processing question: {e}")
            return AskResponse(
                answer="I encountered an error processing your question. Please try asking about specific hospitals, procedures, or costs. For example: 'Find cheap hospitals for knee surgery in NYC'",
                sql_query=None,
                data_used=None
            )

    async def _try_fallback_searches(self, db: AsyncSession, question: str) -> Optional[AskResponse]:
        """Try broader searches when initial query returns no results"""
        
        location_info = self._extract_location_info(question)
        procedure_info = self._extract_procedure_info(question)
        
        fallback_queries = []
        
        # If we have a specific ZIP code, try broader area searches
        if 'zip_code' in location_info:
            zip_code = location_info['zip_code']
            
            # Try ZIP code prefix (broader area)
            if len(zip_code) >= 3:
                prefix = zip_code[:3]
                fallback_queries.append(f"""
                    SELECT p.provider_name, p.provider_city, p.provider_zip_code, p.average_covered_charges, p.ms_drg_definition
                    FROM providers p 
                    WHERE (p.ms_drg_definition ILIKE '%knee%' OR p.ms_drg_definition ILIKE '%470%')
                    AND p.provider_zip_code LIKE '{prefix}%'
                    ORDER BY p.average_covered_charges ASC 
                    LIMIT 5
                """)
        
        # Try city-based search
        if 'city' in location_info:
            city = location_info['city']
            city_patterns = {
                'manhattan': "provider_city ILIKE '%new york%' OR provider_city ILIKE '%manhattan%'",
                'nyc': "provider_city ILIKE '%new york%'",
                'new york': "provider_city ILIKE '%new york%'",
                'brooklyn': "provider_city ILIKE '%brooklyn%'",
                'bronx': "provider_city ILIKE '%bronx%'"
            }
            
            if city.lower() in city_patterns:
                city_condition = city_patterns[city.lower()]
                fallback_queries.append(f"""
                    SELECT p.provider_name, p.provider_city, p.provider_zip_code, p.average_covered_charges, p.ms_drg_definition
                    FROM providers p 
                    WHERE (p.ms_drg_definition ILIKE '%knee%' OR p.ms_drg_definition ILIKE '%470%')
                    AND ({city_condition})
                    ORDER BY p.average_covered_charges ASC 
                    LIMIT 5
                """)
        
        # Try broader procedure search if specific procedure fails
        if procedure_info and any(proc in ['470', '469', '468'] for proc in procedure_info):
            fallback_queries.append(f"""
                SELECT p.provider_name, p.provider_city, p.provider_zip_code, p.average_covered_charges, p.ms_drg_definition
                FROM providers p 
                WHERE p.ms_drg_definition ILIKE '%joint%' OR p.ms_drg_definition ILIKE '%replacement%' OR p.ms_drg_definition ILIKE '%orthopedic%'
                ORDER BY p.average_covered_charges ASC 
                LIMIT 5
            """)
        
        # Execute fallback queries
        for fallback_query in fallback_queries:
            try:
                result = await db.execute(text(fallback_query))
                rows = result.fetchall()
                if rows:
                    columns = list(result.keys())
                    data_used = []
                    for row in rows:
                        row_dict = {}
                        for i, column in enumerate(columns):
                            value = row[i] if i < len(row) else None
                            if isinstance(value, (int, float, str)):
                                row_dict[column] = value
                            else:
                                row_dict[column] = str(value) if value is not None else None
                        data_used.append(row_dict)
                    
                    # Generate answer with context about broader search
                    broader_answer = await self._generate_broader_search_answer(question, data_used)
                    
                    return AskResponse(
                        answer=broader_answer,
                        sql_query=fallback_query,
                        data_used=data_used[:5]
                    )
            except Exception as e:
                logger.error(f"Fallback query failed: {e}")
                continue
        
        return None

    def _generate_helpful_no_results_message(self, question: str) -> str:
        """Generate a helpful message when no results are found"""
        
        location_info = self._extract_location_info(question)
        procedure_info = self._extract_procedure_info(question)
        
        # Extract what we know about their search
        procedure_text = ""
        location_text = ""
        
        if 'knee' in question.lower() or '470' in question:
            procedure_text = "knee replacement procedures"
        elif 'heart' in question.lower() or 'cardiac' in question.lower():
            procedure_text = "cardiac procedures"
        elif 'hip' in question.lower():
            procedure_text = "hip replacement procedures"
        else:
            procedure_text = "the requested procedures"
        
        if 'zip_code' in location_info:
            zip_code = location_info['zip_code']
            location_text = f"ZIP code {zip_code}"
        elif 'city' in location_info:
            city = location_info['city']
            location_text = f"the {city} area"
        else:
            location_text = "that specific location"
        
        # Create helpful suggestions
        suggestions = []
        
        if 'zip_code' in location_info:
            zip_code = location_info['zip_code']
            if zip_code.startswith('100'):
                suggestions.append("Try searching for 'knee replacement in Manhattan' or 'knee replacement in NYC'")
            else:
                suggestions.append("Try searching with a broader area like 'New York' instead of the specific ZIP code")
        
        if procedure_text:
            suggestions.append(f"Consider asking about '{procedure_text} in NYC area' for broader results")
        
        suggestions.append("Use our regular search with a larger radius: try the /providers endpoint with radius_km=100")
        
        suggestion_text = " You could also try: " + "; ".join(suggestions[:2]) if suggestions else ""
        
        return f"I couldn't find any {procedure_text} specifically in {location_text}. This might be because hospitals aren't located in that exact area, or the specific procedure isn't available there.{suggestion_text}"

    async def _generate_broader_search_answer(self, question: str, data: List[Dict[str, Any]]) -> str:
        """Generate answer for broader search results"""
        
        if not data:
            return "I couldn't find any matching results even with a broader search."
        
        try:
            formatted_data = []
            for item in data[:3]:  # Top 3 results
                formatted_item = {}
                for key, value in item.items():
                    if isinstance(value, float) and ('charge' in key.lower() or 'payment' in key.lower() or 'cost' in key.lower()):
                        formatted_item[key] = f"${value:,.2f}"
                    else:
                        formatted_item[key] = value
                formatted_data.append(formatted_item)
            
            data_summary = json.dumps(formatted_data, indent=2)
        except Exception as e:
            logger.error(f"Error formatting broader search data: {e}")
            data_summary = str(data[:3])
        
        prompt = f"""
        The user asked: {question}
        
        I couldn't find results for their exact location, but found these options in the broader area:
        
        {data_summary}
        
        Provide a helpful response that:
        1. Acknowledges that these are results from a broader area since the exact location had no matches
        2. Lists the top 2-3 options with names, locations, and costs
        3. Suggests they might want to search with a larger radius for more options
        4. Keep it conversational and helpful
        
        Answer:
        """
        
        try:
            response = await self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are a helpful healthcare assistant. Acknowledge when showing broader search results."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=300,
                temperature=0.3
            )
            
            return response.choices[0].message.content.strip()
            
        except Exception as e:
            logger.error(f"Error generating broader search answer: {e}")
            return f"I couldn't find matches in your exact location, but found {len(data)} options in the broader area. The most affordable option is {data[0].get('provider_name', 'N/A')} at ${data[0].get('average_covered_charges', 0):,.2f}."
    
    def _is_healthcare_related(self, question: str) -> bool:
        """Check if question is related to healthcare/hospital information"""
        healthcare_keywords = [
            'hospital', 'provider', 'doctor', 'medical', 'surgery', 'procedure',
            'drg', 'cost', 'price', 'cheap', 'expensive', 'rating', 'quality',
            'treatment', 'cardiac', 'heart', 'knee', 'hip', 'replacement',
            'emergency', 'discharge', 'medicare', 'patient', 'clinic', 'health',
            'surgical', 'operation', 'diagnosis', 'therapy', 'care'
        ]
        
        question_lower = question.lower()
        return any(keyword in question_lower for keyword in healthcare_keywords)
    
    def _extract_location_info(self, question: str) -> Dict[str, Any]:
        """Extract location information from the question"""
        location_info = {}
        
        # Look for ZIP codes (5 digits)
        zip_match = re.search(r'\b\d{5}\b', question)
        if zip_match:
            location_info['zip_code'] = zip_match.group()
        
        # Look for city names (common ones)
        cities = ['new york', 'nyc', 'manhattan', 'brooklyn', 'bronx', 'queens', 
                 'albany', 'buffalo', 'syracuse', 'rochester', 'long island']
        question_lower = question.lower()
        for city in cities:
            if city in question_lower:
                location_info['city'] = city
                break
        
        # Look for distance indicators
        distance_match = re.search(r'(\d+)\s*(miles?|km|kilometers?)', question.lower())
        if distance_match:
            distance = int(distance_match.group(1))
            unit = distance_match.group(2)
            if 'mile' in unit:
                location_info['radius_km'] = int(distance * 1.60934)  # Convert miles to km
            else:
                location_info['radius_km'] = distance
        
        return location_info
    
    def _extract_procedure_info(self, question: str) -> List[str]:
        """Extract procedure/DRG information from the question"""
        procedures = []
        question_lower = question.lower()
        
        # Look for DRG codes directly
        drg_matches = re.findall(r'drg\s*(\d+)', question_lower)
        procedures.extend(drg_matches)
        
        # Look for procedure keywords
        for keyword, drg_codes in self.drg_mappings.items():
            if keyword in question_lower:
                procedures.extend(drg_codes)
        
        return list(set(procedures))  # Remove duplicates
    
    async def _generate_sql(self, question: str) -> Optional[str]:
        """Generate SQL query from natural language using OpenAI"""
        
        # Extract structured information from the question
        location_info = self._extract_location_info(question)
        procedure_info = self._extract_procedure_info(question)
        
        schema_info = """
        Database Schema:
        
        providers table:
        - id: Primary key
        - provider_id: CMS provider identifier (string)
        - provider_name: Hospital name (string)
        - provider_city: City name (string)
        - provider_state: State abbreviation (string, usually 'NY')
        - provider_zip_code: ZIP code (string)
        - ms_drg_definition: DRG procedure description (text)
        - total_discharges: Number of procedures performed (integer)
        - average_covered_charges: Hospital charges in dollars (float)
        - average_total_payments: Total payments in dollars (float)  
        - average_medicare_payments: Medicare portion in dollars (float)
        - latitude: Geographic latitude (float)
        - longitude: Geographic longitude (float)
        
        ratings table:
        - id: Primary key
        - provider_id: References providers.provider_id (string)
        - rating: Rating from 1.0 to 10.0 (float)
        - category: Rating category like 'overall', 'cardiac', 'orthopedic' (string)
        
        Common DRG Codes:
        - 470: Major Joint Replacement (knee, hip)
        - 247: Percutaneous Cardiovascular Procedure  
        - 292: Heart Failure & Shock
        - 690: Kidney & Urinary Tract Infections
        """
        
        context = f"""
        Extracted Information:
        - Location: {location_info}
        - Procedures: {procedure_info}
        
        Question: {question}
        """
        
        prompt = f"""
        You are a SQL expert for a healthcare database. Generate a PostgreSQL query for this question.
        
        {schema_info}
        
        {context}
        
        IMPORTANT RULES:
        1. Return ONLY the SQL query, no explanations or markdown
        2. Use proper JOIN syntax when combining tables
        3. For cost queries, ORDER BY average_covered_charges ASC (cheapest first)  
        4. For rating queries, JOIN with ratings table and use AVG(r.rating)
        5. For location searches, use SMART geographic matching:
           - For ZIP codes: Use LIKE patterns (e.g., provider_zip_code LIKE '100%') instead of exact matches
           - For cities: Use ILIKE with wildcards for flexible city matching
           - Prefer broader geographic areas over exact matches
        6. For DRG matching, use ms_drg_definition ILIKE with wildcards
        7. Always GROUP BY all non-aggregate columns when using aggregates
        8. LIMIT results to 20 or fewer
        9. Handle text searches with ILIKE and % wildcards
        10. Use meaningful column aliases for calculated fields
        
        LOCATION MATCHING EXAMPLES:
        - ZIP 10001 → Use "provider_zip_code LIKE '100%'" (Manhattan area)
        - "Manhattan" or "NYC" → Use "provider_city ILIKE '%new york%'"
        - "Brooklyn" → Use "provider_city ILIKE '%brooklyn%'"
        
        Example Patterns:
        
        Smart Location Query:
        SELECT p.provider_name, p.provider_city, p.provider_zip_code, p.average_covered_charges, p.ms_drg_definition
        FROM providers p 
        WHERE (p.ms_drg_definition ILIKE '%knee%' OR p.ms_drg_definition ILIKE '%470%')
        AND p.provider_zip_code LIKE '100%'
        ORDER BY p.average_covered_charges ASC 
        LIMIT 10;
        
        Rating Query:
        SELECT p.provider_name, p.provider_city, AVG(r.rating) as avg_rating, p.ms_drg_definition
        FROM providers p 
        JOIN ratings r ON p.provider_id = r.provider_id
        WHERE p.ms_drg_definition ILIKE '%cardiac%'
        GROUP BY p.provider_id, p.provider_name, p.provider_city, p.ms_drg_definition
        ORDER BY AVG(r.rating) DESC 
        LIMIT 10;
        
        SQL Query:
        """
        
        try:
            response = await self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are a SQL expert. Generate smart geographic queries using LIKE patterns instead of exact matches for better results."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=800,
                temperature=0.1
            )
            
            sql_query = response.choices[0].message.content.strip()
            
            # Clean up the SQL query
            sql_query = re.sub(r'^```sql\s*', '', sql_query, flags=re.IGNORECASE | re.MULTILINE)
            sql_query = re.sub(r'^```\s*', '', sql_query, flags=re.MULTILINE)
            sql_query = re.sub(r'\s*```$', '', sql_query, flags=re.MULTILINE)
            sql_query = sql_query.strip()
            
            # Basic validation - ensure it's a SELECT query
            if not sql_query.upper().startswith('SELECT'):
                logger.warning(f"Generated query doesn't start with SELECT: {sql_query}")
                return None
            
            # Remove potentially dangerous statements
            dangerous_keywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE', 'TRUNCATE']
            sql_upper = sql_query.upper()
            if any(keyword in sql_upper for keyword in dangerous_keywords):
                logger.warning(f"Generated query contains dangerous keywords: {sql_query}")
                return None
            
            return sql_query
            
        except Exception as e:
            logger.error(f"Error generating SQL: {e}")
            return None
    
    async def _generate_answer(self, question: str, data: List[Dict[str, Any]]) -> str:
        """Generate natural language answer from query results"""
        
        if not data:
            return "I couldn't find any matching results for your question."
        
        # Prepare data summary
        try:
            # Format monetary values properly
            formatted_data = []
            for item in data[:5]:  # Top 5 results
                formatted_item = {}
                for key, value in item.items():
                    if isinstance(value, float):
                        if 'charge' in key.lower() or 'payment' in key.lower() or 'cost' in key.lower():
                            formatted_item[key] = f"${value:,.2f}"
                        elif 'rating' in key.lower():
                            formatted_item[key] = f"{value:.1f}/10"
                        else:
                            formatted_item[key] = round(value, 2)
                    else:
                        formatted_item[key] = value
                formatted_data.append(formatted_item)
            
            data_summary = json.dumps(formatted_data, indent=2)
        except Exception as e:
            logger.error(f"Error formatting data: {e}")
            data_summary = str(data[:3])  # Fallback to simple string representation
        
        prompt = f"""
        Based on the following hospital data, provide a helpful answer to the user's question.
        
        User Question: {question}
        
        Hospital Data:
        {data_summary}
        
        Instructions:
        1. Give a direct, conversational answer
        2. Include specific hospital names and key details
        3. Format costs as currency (e.g., $25,000)
        4. Mention ratings clearly (e.g., "8.5/10 rating")
        5. Keep response concise but informative (2-4 sentences)
        6. If showing multiple options, highlight the top 2-3
        7. Don't mention technical database details
        
        Answer:
        """
        
        try:
            response = await self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are a helpful healthcare assistant providing clear information about hospitals and costs."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=400,
                temperature=0.3
            )
            
            return response.choices[0].message.content.strip()
            
        except Exception as e:
            logger.error(f"Error generating answer: {e}")
            return f"I found {len(data)} results but had trouble formatting the response. The data shows various hospitals with different costs and ratings for your query."
    
    def get_example_prompts(self) -> List[str]:
        """Return example prompts that the AI can handle"""
        return [
            "Who is the cheapest for DRG 470 within 25 miles of 10001?",
            "What are the best rated hospitals for heart surgery in New York?", 
            "Show me hospitals with lowest cost for knee replacement",
            "Which providers have the highest ratings for cardiac procedures?",
            "Find hospitals near ZIP code 10032 with good ratings",
            "What's the average cost for major joint replacement in NYC?",
            "Compare costs between hospitals for hip surgery",
            "Which hospital has the best value for knee replacement surgery?",
            "Show me emergency care options with high ratings",
            "Find affordable cardiac procedures in Manhattan"
        ]