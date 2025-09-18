"""
Sample queries for testing the API endpoints
"""
import asyncio
import aiohttp
import json

BASE_URL = "http://localhost:8000"

async def test_providers_endpoint():
    """Test the providers search endpoint"""
    async with aiohttp.ClientSession() as session:
        
        # Test 1: Search for knee replacement procedures
        params = {
            "drg": "470",
            "zip": "10001", 
            "radius_km": 50
        }
        
        async with session.get(f"{BASE_URL}/providers", params=params) as response:
            if response.status == 200:
                data = await response.json()
                print("✅ Providers search successful")
                print(f"Found {len(data)} providers")
                if data:
                    print(f"Cheapest: {data[0]['provider_name']} - ${data[0]['average_covered_charges']:,.2f}")
            else:
                print(f"❌ Providers search failed: {response.status}")

async def test_ask_endpoint():
    """Test the AI assistant endpoint"""
    async with aiohttp.ClientSession() as session:
        
        test_questions = [
            "Who is cheapest for knee replacement near 10001?",
            "What are the best rated hospitals for heart surgery?",
            "Show me hospitals with lowest cost for DRG 470",
            "What's the weather today?",  # Out of scope
        ]
        
        for question in test_questions:
            payload = {"question": question}
            
            async with session.post(f"{BASE_URL}/ask", json=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    print(f"✅ Question: {question}")
                    print(f"Answer: {data['answer'][:100]}...")
                    print()
                else:
                    print(f"❌ Question failed: {question}")

async def main():
    """Run all sample queries"""
    print("Testing Healthcare Cost Navigator API")
    print("=" * 50)
    
    await test_providers_endpoint()
    print()
    await test_ask_endpoint()

if __name__ == "__main__":
    asyncio.run(main())