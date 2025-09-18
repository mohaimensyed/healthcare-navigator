from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
import openai
import json
import os
from typing import Dict, List, Any
import re

from app.schemas import AskResponse

class AIService:
    def __init__(self):
        self.client = openai.AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        
    async def process_question(self, db: AsyncSession, question: str) -> AskResponse:
        """
        Process natural language question and return grounded answer
        """
        
        # Check if question is in scope
        if not self._is_healthcare_related(question):
            return AskResponse(
                answer="I can only help with hospital pricing and quality information. Please ask about medical procedures, costs, or hospital ratings.",
                sql_query=None,
                data_used=None
            )
        
        try:
            # Generate SQL query from natural language
            sql_query = await self._generate_sql(question)
            
            if not sql_query:
                return AskResponse(
                    answer="I couldn't understand your question. Please try asking about specific procedures, costs, or hospital ratings.",
                    sql_query=None,
                    data_used=None
                )
            
            # Execute the SQL query
            result = await db.execute(text(sql_query))
            rows = result.fetchall()
            
            if not rows:
                return AskResponse(
                    answer="I couldn't find any data matching your criteria. Please try a different search.",
                    sql_query=sql_query,
                    data_used=[]
                )
            
            # Convert results to dictionaries
            columns = result.keys()
            data_used = [dict(zip(columns, row)) for row in rows]
            
            # Generate natural language answer
            answer = await self._generate_answer(question, data_used)
            
            return AskResponse(
                answer=answer,
                sql_query=sql_query,
                data_used=data_used[:10]  # Limit to first 10 results for response
            )
            
        except Exception as e:
            print(f"Error processing question: {e}")
            return AskResponse(
                answer="I encountered an error processing your question. Please try rephrasing it.",
                sql_query=None,
                data_used=None
            )
    
    def _is_healthcare_related(self, question: str) -> bool:
        """Check if question is related to healthcare/hospital information"""
        healthcare_keywords = [
            'hospital', 'provider', 'doctor', 'medical', 'surgery', 'procedure',
            'drg', 'cost', 'price', 'cheap', 'expensive', 'rating', 'quality',
            'treatment', 'cardiac', 'heart', 'knee', 'hip', 'replacement',
            'emergency', 'discharge', 'medicare', 'patient', 'clinic'
        ]
        
        question_lower = question.lower()
        return any(keyword in question_lower for keyword in healthcare_keywords)
    
    async def _generate_sql(self, question: str) -> str:
        """Generate SQL query from natural language using OpenAI"""
        
        schema_info = """
        Available tables and columns:
        
        providers table:
        - provider_id (string): CMS ID for the hospital
        - provider_name (string): Hospital name
        - provider_city (string): Hospital city
        - provider_state (string): Hospital state  
        - provider_zip_code (string): Hospital ZIP code
        - ms_drg_definition (string): Procedure description (e.g., "470 - Major Joint Replacement w/o MCC")
        - total_discharges (integer): Number of procedures
        - average_covered_charges (float): Average hospital charges
        - average_total_payments (float): Total payments
        - average_medicare_payments (float): Medicare payments
        - latitude (float): Hospital latitude
        - longitude (float): Hospital longitude
        
        ratings table:
        - provider_id (string): Foreign key to providers
        - rating (float): Rating from 1-10
        - category (string): Rating category (overall, cardiac, orthopedic, etc.)
        
        Common DRG codes:
        - 470: Major Joint Replacement (knee, hip)
        - 247: Percutaneous Cardiovascular Procedure
        - 292: Heart Failure & Shock
        - 690: Kidney & Urinary Tract Infections
        """
        
        prompt = f"""
        You are a SQL expert for a healthcare database. Convert the following natural language question into a SQL query.
        
        Database Schema:
        {schema_info}
        
        Question: {question}
        
        Rules:
        1. Only return the SQL query, no explanations
        2. Use proper JOIN syntax when needed
        3. For location-based queries, use ZIP codes or city names
        4. For DRG matching, use ILIKE with partial matches
        5. For cost queries, sort by average_covered_charges
        6. For rating queries, join with ratings table and use AVG()
        7. Limit results to reasonable numbers (10-50)
        8. Use PostgreSQL syntax
        
        Example queries:
        - "cheapest hospitals for knee replacement near 10001" → 
          SELECT p.*, AVG(r.rating) as avg_rating FROM providers p 
          LEFT JOIN ratings r ON p.provider_id = r.provider_id 
          WHERE p.ms_drg_definition ILIKE '%knee%' OR p.ms_drg_definition ILIKE '%470%'
          GROUP BY p.id ORDER BY p.average_covered_charges ASC LIMIT 10
        
        SQL Query:
        """
        
        try:
            response = await self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are a SQL expert. Return only valid PostgreSQL queries."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=500,
                temperature=0.1
            )
            
            sql_query = response.choices[0].message.content.strip()
            
            # Clean up the SQL query
            sql_query = re.sub(r'^```sql\s*', '', sql_query, flags=re.IGNORECASE)
            sql_query = re.sub(r'\s*```$', '', sql_query)
            sql_query = sql_query.strip()
            
            return sql_query
            
        except Exception as e:
            print(f"Error generating SQL: {e}")
            return None
    
    async def _generate_answer(self, question: str, data: List[Dict[str, Any]]) -> str:
        """Generate natural language answer from query results"""
        
        if not data:
            return "I couldn't find any matching results for your question."
        
        # Prepare data summary
        data_summary = json.dumps(data[:5], indent=2, default=str)  # Top 5 results
        
        prompt = f"""
        Based on the following database results, provide a natural, helpful answer to the user's question.
        
        User Question: {question}
        
        Database Results:
        {data_summary}
        
        Instructions:
        1. Provide a direct, conversational answer
        2. Include specific hospital names, costs, and ratings when relevant
        3. If showing costs, format them as currency (e.g., $25,000)
        4. If showing ratings, mention them clearly (e.g., "rating: 8.5/10")
        5. Keep the response concise but informative
        6. Don't mention technical details like SQL queries or table names
        
        Answer:
        """
        
        try:
            response = await self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are a helpful healthcare assistant. Provide clear, accurate answers based on hospital data."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=300,
                temperature=0.3
            )
            
            return response.choices[0].message.content.strip()
            
        except Exception as e:
            print(f"Error generating answer: {e}")
            return "I found some results but had trouble formatting the answer. Please try rephrasing your question."
    
    def get_example_prompts(self) -> List[str]:
        """Return example prompts that the AI can handle"""
        return [
            "Who is the cheapest for DRG 470 within 25 miles of 10001?",
            "What are the best rated hospitals for heart surgery in New York?",
            "Show me hospitals with lowest cost for knee replacement",
            "Which providers have the highest ratings for cardiac procedures?",
            "Find hospitals near ZIP code 10032 with good ratings",
            "What's the average cost for major joint replacement in NYC?",
            "Compare costs between hospitals for DRG 470",
            "Which hospital has the best value (cost vs rating) for knee surgery?"
        ]