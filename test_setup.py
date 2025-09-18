# test_setup.py - Quick test to verify your setup
"""
Run this script to test if everything is set up correctly:
python test_setup.py
"""

import sys
import os
from pathlib import Path

def test_imports():
    """Test if all required packages are installed"""
    print("Testing imports...")
    
    try:
        import fastapi
        print("✅ FastAPI installed")
    except ImportError:
        print("❌ FastAPI not installed - run: pip install fastapi")
        return False
    
    try:
        import sqlalchemy
        print("✅ SQLAlchemy installed")
    except ImportError:
        print("❌ SQLAlchemy not installed")
        return False
    
    try:
        import openai
        print("✅ OpenAI installed")
    except ImportError:
        print("❌ OpenAI not installed")
        return False
    
    return True

def test_project_structure():
    """Test if project files exist"""
    print("\nTesting project structure...")
    
    required_files = [
        "app/__init__.py",
        "app/main.py", 
        "app/models.py",
        "app/database.py",
        "app/schemas.py",
        "app/init_db.py",
        "app/services/__init__.py",
        "app/services/provider_service.py",
        "app/services/ai_service.py",
        "requirements.txt",
        ".env"
    ]
    
    missing_files = []
    for file_path in required_files:
        if Path(file_path).exists():
            print(f"✅ {file_path}")
        else:
            print(f"❌ {file_path} - MISSING")
            missing_files.append(file_path)
    
    return len(missing_files) == 0

def test_environment():
    """Test environment variables"""
    print("\nTesting environment variables...")
    
    from dotenv import load_dotenv
    load_dotenv()
    
    database_url = os.getenv("DATABASE_URL")
    openai_key = os.getenv("OPENAI_API_KEY")
    
    if database_url:
        print("✅ DATABASE_URL found")
    else:
        print("❌ DATABASE_URL not found in .env file")
    
    if openai_key and openai_key != "your_openai_api_key_here":
        print("✅ OPENAI_API_KEY found")
    else:
        print("❌ OPENAI_API_KEY not set in .env file")
    
    return bool(database_url and openai_key and openai_key != "your_openai_api_key_here")

def test_app_imports():
    """Test if app modules can be imported"""
    print("\nTesting app imports...")
    
    try:
        from app.main import app
        print("✅ app.main imports successfully")
        return True
    except ImportError as e:
        print(f"❌ Failed to import app.main: {e}")
        return False

def main():
    """Run all tests"""
    print("Healthcare Navigator Setup Test")
    print("=" * 40)
    
    all_tests_passed = True
    
    # Run tests
    all_tests_passed &= test_imports()
    all_tests_passed &= test_project_structure() 
    all_tests_passed &= test_environment()
    all_tests_passed &= test_app_imports()
    
    print("\n" + "=" * 40)
    
    if all_tests_passed:
        print("🎉 ALL TESTS PASSED! Your setup is ready.")
        print("\nTo start the app, run:")
        print("uvicorn app.main:app --reload")
    else:
        print("❌ Some tests failed. Fix the issues above and try again.")
        print("\nCommon fixes:")
        print("- Make sure virtual environment is activated: source venv/Scripts/activate")
        print("- Install dependencies: pip install -r requirements.txt")
        print("- Set up .env file with your OpenAI API key")
        print("- Check that all code files are created")

if __name__ == "__main__":
    main()