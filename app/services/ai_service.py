from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
import openai
import json
import os
from typing import Dict, List, Any, Optional
import re
import logging
import math

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
        
        # Enhanced DRG mappings with more procedures
        self.drg_mappings = {
            'knee': ['470', '469', '468', '489', '488'],
            'hip': ['470', '469', '468'],
            'joint replacement': ['470', '469', '468'],
            'heart': ['246', '247', '248', '249', '250', '251', '252', '280', '281', '282'],
            'cardiac': ['246', '247', '248', '249', '250', '251', '252', '280', '281', '282'],
            'cardiovascular': ['246', '247', '248', '249', '250', '251', '252'],
            'bypass': ['231', '232', '233', '234', '235', '236'],
            'kidney': ['682', '683', '684', '685', '686', '687'],
            'dialysis': ['682', '683', '684', '685'],
            'emergency': ['981', '982', '983', '984'],
            'surgery': ['470', '480', '481', '482'],
            'cancer': ['834', '835', '836', '837', '838'],
            'pneumonia': ['177', '178', '179', '193', '194', '195'],
            'stroke': ['061', '062', '063', '064', '065', '066'],
            'maternity': ['765', '766', '767', '768', '774', '775']
        }

        # NYC area ZIP code patterns for smarter location matching
        self.nyc_zip_patterns = {
            '100': 'Manhattan', '101': 'Manhattan', '102': 'Manhattan',
            '112': 'Brooklyn', '113': 'Brooklyn',
            '104': 'Bronx', '105': 'Bronx',
            '114': 'Queens', '115': 'Queens', '116': 'Queens',
            '103': 'Staten Island'
        }
        
        # Query intent patterns for better SQL generation
        self.intent_patterns = {
            'cheapest': ['cheapest', 'lowest cost', 'most affordable', 'least expensive', 'budget'],
            'best_rated': ['best rated', 'highest rated', 'top rated', 'best quality', 'highest quality'],
            'nearest': ['nearest', 'closest', 'nearby', 'close to', 'near me'],
            'comparison': ['compare', 'versus', 'vs', 'difference between'],
            'value': ['best value', 'value for money', 'cost effective', 'bang for buck']
        }
        
    async def process_question(self, db: AsyncSession, question: str) -> AskResponse:
        """Process natural language question with enhanced ranking consistency"""
        
        logger.info(f"Processing question: {question}")
        
        # Check if question is in scope
        if not self._is_healthcare_related(question):
            return AskResponse(
                answer="I can only help with hospital pricing and quality information. Please ask about medical procedures, costs, or hospital ratings. For example: 'What are the cheapest hospitals for knee replacement?' or 'Which hospitals have the best ratings for cardiac surgery?'",
                sql_query=None,
                data_used=None
            )
        
        try:
            # Detect query intent for better ranking
            intent = self._detect_query_intent(question)
            
            # Generate SQL query from natural language
            sql_query = await self._generate_sql(question, intent)
            
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
                fallback_response = await self._try_fallback_searches(db, question, intent)
                if fallback_response:
                    return fallback_response
                
                # Generate helpful "no results" message
                helpful_message = self._generate_helpful_no_results_message(question)
                return AskResponse(
                    answer=helpful_message,
                    sql_query=sql_query,
                    data_used=[]
                )
            
            # Convert results to dictionaries with enhanced data processing
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
            
            # Apply composite ranking if needed (for value-based queries)
            if intent == 'value' and data_used:
                data_used = self._apply_composite_ranking(data_used)
            
            # Generate natural language answer with intent consideration
            answer = await self._generate_answer(question, data_used, intent)
            
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

    def _detect_query_intent(self, question: str) -> str:
        """Detect the intent of the user's query for better ranking"""
        question_lower = question.lower()
        
        for intent, keywords in self.intent_patterns.items():
            if any(keyword in question_lower for keyword in keywords):
                return intent
        
        # Default intent based on common patterns
        if any(word in question_lower for word in ['cost', 'price', 'cheap', 'affordable']):
            return 'cheapest'
        elif any(word in question_lower for word in ['rating', 'quality', 'best']):
            return 'best_rated'
        elif any(word in question_lower for word in ['near', 'close', 'distance']):
            return 'nearest'
        else:
            return 'value'  # Default to value-based ranking

    def _apply_composite_ranking(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Apply the same composite ranking logic as provider_service"""
        
        def calculate_score(item):
            try:
                # Cost score (inverse relationship)
                cost = max(float(item.get('average_covered_charges', 50000)), 1000)
                cost_score = 1000000 / cost
                
                # Rating score
                rating = float(item.get('average_rating') or item.get('avg_rating', 5.0))
                rating_score = rating * 15
                
                # Distance score (if available)
                distance = float(item.get('distance_km', 50))
                distance_score = max(0, 100 - (distance * 1.5))
                
                # Volume score
                volume = int(item.get('total_discharges', 0))
                volume_score = min(math.log(volume + 1) * 10, 50)
                
                # Composite score with same weights as provider_service
                return (cost_score * 0.4 + rating_score * 0.35 + 
                       distance_score * 0.15 + volume_score * 0.1)
                       
            except Exception as e:
                logger.error(f"Error calculating score: {e}")
                return 0.0
        
        return sorted(data, key=calculate_score, reverse=True)

    async def _try_fallback_searches(self, db: AsyncSession, question: str, intent: str) -> Optional[AskResponse]:
        """Enhanced fallback searches with intent consideration"""
        
        location_info = self._extract_location_info(question)
        procedure_info = self._extract_procedure_info(question)
        
        fallback_queries = []
        
        # Intent-based fallback strategies
        if intent == 'cheapest':
            base_order = "ORDER BY p.average_covered_charges ASC"
        elif intent == 'best_rated':
            base_order = "ORDER BY AVG(r.rating) DESC"
        elif intent == 'nearest':
            base_order = "ORDER BY p.provider_zip_code ASC"  # Approximate by ZIP
        else:  # value-based
            base_order = """ORDER BY (
                (1000000 / GREATEST(p.average_covered_charges, 1000)) * 0.4 +
                COALESCE(AVG(r.rating), 5.0) * 15 * 0.35 +
                COALESCE(p.total_discharges, 0) * 0.01
            ) DESC"""
        
        # Broader geographic search
        if 'zip_code' in location_info:
            zip_code = location_info['zip_code']
            if len(zip_code) >= 3:
                prefix = zip_code[:3]
                fallback_queries.append(f"""
                    SELECT p.provider_name, p.provider_city, p.provider_zip_code, 
                           p.average_covered_charges, p.ms_drg_definition, p.total_discharges,
                           AVG(r.rating) as avg_rating
                    FROM providers p 
                    LEFT JOIN ratings r ON p.provider_id = r.provider_id
                    WHERE (p.ms_drg_definition ILIKE '%knee%' OR p.ms_drg_definition ILIKE '%joint%')
                    AND p.provider_zip_code LIKE '{prefix}%'
                    GROUP BY p.id, p.provider_name, p.provider_city, p.provider_zip_code, 
                             p.average_covered_charges, p.ms_drg_definition, p.total_discharges
                    {base_order}
                    LIMIT 5
                """)
        
        # Broader procedure search
        if procedure_info:
            fallback_queries.append(f"""
                SELECT p.provider_name, p.provider_city, p.provider_zip_code, 
                       p.average_covered_charges, p.ms_drg_definition, p.total_discharges,
                       AVG(r.rating) as avg_rating
                FROM providers p 
                LEFT JOIN ratings r ON p.provider_id = r.provider_id
                WHERE p.ms_drg_definition ILIKE '%orthopedic%' 
                   OR p.ms_drg_definition ILIKE '%replacement%' 
                   OR p.ms_drg_definition ILIKE '%surgery%'
                GROUP BY p.id, p.provider_name, p.provider_city, p.provider_zip_code, 
                         p.average_covered_charges, p.ms_drg_definition, p.total_discharges
                {base_order}
                LIMIT 5
            """)
        
        # City-based search
        if 'city' in location_info:
            city = location_info['city']
            city_patterns = {
                'manhattan': "p.provider_city ILIKE '%new york%' OR p.provider_city ILIKE '%manhattan%'",
                'nyc': "p.provider_city ILIKE '%new york%'",
                'new york': "p.provider_city ILIKE '%new york%'",
                'brooklyn': "p.provider_city ILIKE '%brooklyn%'",
                'bronx': "p.provider_city ILIKE '%bronx%'"
            }
            
            if city.lower() in city_patterns:
                city_condition = city_patterns[city.lower()]
                fallback_queries.append(f"""
                    SELECT p.provider_name, p.provider_city, p.provider_zip_code, 
                           p.average_covered_charges, p.ms_drg_definition, p.total_discharges,
                           AVG(r.rating) as avg_rating
                    FROM providers p 
                    LEFT JOIN ratings r ON p.provider_id = r.provider_id
                    WHERE ({city_condition})
                    GROUP BY p.id, p.provider_name, p.provider_city, p.provider_zip_code, 
                             p.average_covered_charges, p.ms_drg_definition, p.total_discharges
                    {base_order}
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
                    
                    # Apply composite ranking for value queries
                    if intent == 'value':
                        data_used = self._apply_composite_ranking(data_used)
                    
                    broader_answer = await self._generate_broader_search_answer(question, data_used, intent)
                    
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
        """Generate enhanced helpful message when no results found"""
        
        location_info = self._extract_location_info(question)
        procedure_info = self._extract_procedure_info(question)
        
        procedure_text = "the requested procedures"
        location_text = "that specific location"
        
        # Identify procedure
        if 'knee' in question.lower() or '470' in question:
            procedure_text = "knee replacement procedures"
        elif 'heart' in question.lower() or 'cardiac' in question.lower():
            procedure_text = "cardiac procedures"
        elif 'hip' in question.lower():
            procedure_text = "hip replacement procedures"
        
        # Identify location
        if 'zip_code' in location_info:
            zip_code = location_info['zip_code']
            location_text = f"ZIP code {zip_code}"
        elif 'city' in location_info:
            city = location_info['city']
            location_text = f"the {city} area"
        
        suggestions = [
            f"Try searching for '{procedure_text} in NYC area' for broader results",
            "Consider expanding your search radius to 100km",
            "Use different procedure terms like 'joint replacement' instead of specific DRG codes"
        ]
        
        suggestion_text = " You could try: " + "; ".join(suggestions[:2])
        
        return f"I couldn't find any {procedure_text} specifically in {location_text}. This might be because hospitals aren't located in that exact area, or the specific procedure isn't available there.{suggestion_text}"

    async def _generate_broader_search_answer(self, question: str, data: List[Dict[str, Any]], intent: str) -> str:
        """Generate answer for broader search results with intent consideration"""
        
        if not data:
            return "I couldn't find any matching results even with a broader search."
        
        try:
            # Format data with enhanced presentation
            formatted_data = []
            for item in data[:3]:
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
            logger.error(f"Error formatting broader search data: {e}")
            data_summary = str(data[:3])
        
        intent_context = {
            'cheapest': "most affordable options",
            'best_rated': "highest-rated providers", 
            'nearest': "closest options",
            'value': "best value options (balancing cost and quality)"
        }.get(intent, "best options")
        
        prompt = f"""
        The user asked: {question}
        
        Here are the best {intent_context} in the New York area:
        
        {data_summary}
        
        Provide a helpful response that:
        1. Presents these as the top recommendations (don't mention "broader area" or "exact location")
        2. Lists the top 2-3 options with names, locations, costs, and ratings
        3. Explains the ranking rationale based on the query intent: {intent}
        4. Keeps it conversational and confident
        5. For nearby results, present them as the best available options
        
        Answer:
        """
        
        try:
            response = await self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": f"You are a helpful healthcare assistant. Focus on {intent_context} when presenting results."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=350,
                temperature=0.3
            )
            
            return response.choices[0].message.content.strip()
            
        except Exception as e:
            logger.error(f"Error generating broader search answer: {e}")
            return f"I couldn't find matches in your exact location, but found {len(data)} {intent_context} in the broader area. The top choice is {data[0].get('provider_name', 'N/A')} at ${data[0].get('average_covered_charges', 0):,.2f}."
    
    def _is_healthcare_related(self, question: str) -> bool:
        """Enhanced healthcare topic detection"""
        healthcare_keywords = [
            'hospital', 'provider', 'doctor', 'medical', 'surgery', 'procedure',
            'drg', 'cost', 'price', 'cheap', 'expensive', 'rating', 'quality',
            'treatment', 'cardiac', 'heart', 'knee', 'hip', 'replacement',
            'emergency', 'discharge', 'medicare', 'patient', 'clinic', 'health',
            'surgical', 'operation', 'diagnosis', 'therapy', 'care', 'hospital',
            'cancer', 'oncology', 'pneumonia', 'stroke', 'maternity', 'pediatric'
        ]
        
        question_lower = question.lower()
        return any(keyword in question_lower for keyword in healthcare_keywords)
    
    def _extract_location_info(self, question: str) -> Dict[str, Any]:
        """Enhanced location extraction with better patterns"""
        location_info = {}
        
        # Look for ZIP codes (5 digits, possibly with +4)
        zip_match = re.search(r'\b\d{5}(?:-\d{4})?\b', question)
        if zip_match:
            location_info['zip_code'] = zip_match.group().split('-')[0]  # Just 5 digits
        
        # Enhanced city detection
        cities = [
            'new york', 'nyc', 'manhattan', 'brooklyn', 'bronx', 'queens', 'staten island',
            'albany', 'buffalo', 'syracuse', 'rochester', 'long island', 'westchester',
            'yonkers', 'schenectady', 'troy', 'utica', 'binghamton', 'niagara falls'
        ]
        question_lower = question.lower()
        for city in cities:
            if city in question_lower:
                location_info['city'] = city
                break
        
        # Distance indicators with better parsing
        distance_patterns = [
            r'(\d+)\s*(miles?|mi)\b',
            r'(\d+)\s*(km|kilometers?)\b',
            r'within\s+(\d+)\s*(miles?|mi|km|kilometers?)\b',
            r'(\d+)\s*(mile|kilometer)\s+radius\b'
        ]
        
        for pattern in distance_patterns:
            distance_match = re.search(pattern, question.lower())
            if distance_match:
                distance = int(distance_match.group(1))
                unit = distance_match.group(2)
                if 'mile' in unit or 'mi' in unit:
                    location_info['radius_km'] = int(distance * 1.60934)
                else:
                    location_info['radius_km'] = distance
                break
        
        return location_info
    
    def _extract_procedure_info(self, question: str) -> List[str]:
        """Enhanced procedure extraction with synonyms"""
        procedures = []
        question_lower = question.lower()
        
        # Direct DRG code extraction
        drg_matches = re.findall(r'drg\s*(\d+)', question_lower)
        procedures.extend(drg_matches)
        
        # Enhanced keyword mapping with synonyms
        for keyword, drg_codes in self.drg_mappings.items():
            if keyword in question_lower:
                procedures.extend(drg_codes)
        
        # Additional medical term detection
        medical_terms = {
            'arthroplasty': ['470', '469', '468'],
            'angioplasty': ['246', '247', '248'],
            'appendectomy': ['338', '339', '340'],
            'cholecystectomy': ['417', '418', '419'],
            'hernia': ['353', '354', '355']
        }
        
        for term, codes in medical_terms.items():
            if term in question_lower:
                procedures.extend(codes)
        
        return list(set(procedures))  # Remove duplicates
    
    async def _generate_sql(self, question: str, intent: str) -> Optional[str]:
        """Enhanced SQL generation with intent-aware ranking"""
        
        location_info = self._extract_location_info(question)
        procedure_info = self._extract_procedure_info(question)
        
        # Intent-specific ordering
        order_clauses = {
            'cheapest': 'ORDER BY p.average_covered_charges ASC',
            'best_rated': 'ORDER BY AVG(r.rating) DESC',
            'nearest': 'ORDER BY p.provider_zip_code ASC',  # Approximate
            'value': '''ORDER BY (
                (1000000 / GREATEST(p.average_covered_charges, 1000)) * 0.4 +
                COALESCE(AVG(r.rating), 5.0) * 15 * 0.35 +
                GREATEST(0, 100 - 50) * 0.15 +
                LEAST(LOG(GREATEST(p.total_discharges, 1)) * 10, 50) * 0.1
            ) DESC'''
        }
        
        schema_info = """
        Database Schema:
        
        providers table:
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
        - provider_id: References providers.provider_id (string)
        - rating: Rating from 1.0 to 10.0 (float)
        - category: Rating category like 'overall', 'cardiac', 'orthopedic' (string)
        
        Enhanced DRG Codes:
        - 470: Major Joint Replacement (knee, hip)
        - 247: Percutaneous Cardiovascular Procedure
        - 292: Heart Failure & Shock
        - 690: Kidney & Urinary Tract Infections
        """
        
        context = f"""
        Extracted Information:
        - Location: {location_info}
        - Procedures: {procedure_info}
        - Query Intent: {intent}
        
        Question: {question}
        """
        
        intent_guidance = {
            'cheapest': "Focus on cost-effectiveness. Use ORDER BY average_covered_charges ASC.",
            'best_rated': "Focus on quality ratings. JOIN with ratings table and use ORDER BY AVG(rating) DESC.",
            'nearest': "Focus on location. Use ZIP code proximity or exact location matching.",
            'value': "Focus on value - balance cost, quality, and experience using composite scoring."
        }
        
        prompt = f"""
        You are a SQL expert for a healthcare database. Generate a PostgreSQL query for this question.
        
        {schema_info}
        
        {context}
        
        INTENT GUIDANCE: {intent_guidance.get(intent, 'Balance multiple factors for best results.')}
        
        IMPORTANT RULES:
        1. Return ONLY the SQL query, no explanations or markdown
        2. Use proper JOIN syntax when combining tables: LEFT JOIN ratings r ON p.provider_id = r.provider_id
        3. For cost queries: {order_clauses.get('cheapest', 'ORDER BY p.average_covered_charges ASC')}
        4. For rating queries: {order_clauses.get('best_rated', 'ORDER BY AVG(r.rating) DESC')}
        5. For value queries: {order_clauses.get('value', 'Use composite scoring')}
        6. Use SMART geographic matching with LIKE patterns (e.g., provider_zip_code LIKE '100%')
        7. For DRG matching, use ms_drg_definition ILIKE with wildcards
        8. Always GROUP BY all non-aggregate columns when using aggregates
        9. LIMIT results to 20 or fewer
        10. Include ratings in SELECT when available: AVG(r.rating) as avg_rating
        
        Query Intent: {intent}
        Suggested ORDER BY: {order_clauses.get(intent, order_clauses['value'])}
        
        SQL Query:
        """
        
        try:
            response = await self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": f"Generate SQL optimized for {intent} queries with composite ranking when appropriate."},
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
            
            # Basic validation
            if not sql_query.upper().startswith('SELECT'):
                logger.warning(f"Generated query doesn't start with SELECT: {sql_query}")
                return None
            
            # Security check
            dangerous_keywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE', 'TRUNCATE']
            sql_upper = sql_query.upper()
            if any(keyword in sql_upper for keyword in dangerous_keywords):
                logger.warning(f"Generated query contains dangerous keywords: {sql_query}")
                return None
            
            return sql_query
            
        except Exception as e:
            logger.error(f"Error generating SQL: {e}")
            return None
    
    async def _generate_answer(self, question: str, data: List[Dict[str, Any]], intent: str) -> str:
        """Enhanced answer generation with intent consideration"""
        
        if not data:
            return "I couldn't find any matching results for your question."
        
        try:
            # Enhanced data formatting with intent-specific presentation
            formatted_data = []
            for item in data[:5]:
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
            data_summary = str(data[:3])
        
        intent_instructions = {
            'cheapest': "Focus on the most affordable options and highlight cost savings.",
            'best_rated': "Emphasize quality ratings and explain why these hospitals are top-rated.",
            'nearest': "Highlight proximity and convenience factors.",
            'value': "Explain the balance of cost, quality, and other factors that make these the best value."
        }
        
        prompt = f"""
        Based on the following hospital data, provide a helpful answer to the user's question.
        
        User Question: {question}
        Query Intent: {intent}
        
        Hospital Data:
        {data_summary}
        
        Instructions:
        1. Give a direct, conversational answer optimized for {intent} queries
        2. {intent_instructions.get(intent, 'Provide balanced information')}
        3. Present these as the best available options (don't mention "exact matches" or "fallbacks")
        4. Include specific hospital names and key details
        5. Format costs as currency (e.g., $25,000)
        6. Mention ratings clearly (e.g., "8.5/10 rating")
        7. For value queries, explain the ranking factors (cost, quality, experience)
        8. Keep response concise but informative (3-5 sentences)
        9. Highlight the top 2-3 options based on the intent
        10. Don't mention technical database details or search limitations
        
        Answer:
        """
        
        try:
            response = await self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": f"You are a helpful healthcare assistant specializing in {intent} recommendations."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=400,
                temperature=0.3
            )
            
            return response.choices[0].message.content.strip()
            
        except Exception as e:
            logger.error(f"Error generating answer: {e}")
            return f"I found {len(data)} results for your {intent} query. The top option is {data[0].get('provider_name', 'N/A')} with charges of ${data[0].get('average_covered_charges', 0):,.2f} and a rating of {data[0].get('avg_rating', 'N/A')}/10."
    
    def get_example_prompts(self) -> List[str]:
        """Enhanced example prompts covering different intents"""
        return [
            "Who is the cheapest for DRG 470 within 25 miles of 10001?",
            "What are the best rated hospitals for heart surgery in New York?",
            "Show me the best value hospitals for knee replacement near Manhattan",
            "Which providers have the highest ratings for cardiac procedures?",
            "Find the most affordable orthopedic hospitals with good ratings",
            "What's the closest hospital for emergency care near 10032?",
            "Compare costs between hospitals for hip surgery in NYC",
            "Which hospital offers the best combination of quality and affordability for joint replacement?",
            "Show me top-rated hospitals for cancer treatment in New York",
            "Find cost-effective options for maternity care near Brooklyn"
        ]