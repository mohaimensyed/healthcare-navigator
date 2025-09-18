# services/__init__.py
"""
Service modules for Healthcare Cost Navigator

This package contains business logic services that handle specific domains:
- provider_service: Hospital provider search and management
- ai_service: Natural language processing and AI assistant functionality
"""

from .provider_service import ProviderService
from .ai_service import AIService

__all__ = [
    'ProviderService',
    'AIService',
]

# Version information
__version__ = '1.0.0'
__author__ = 'Healthcare Cost Navigator Team'
__description__ = 'Business logic services for healthcare provider search and AI assistance'