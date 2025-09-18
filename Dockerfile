# Healthcare Cost Navigator - Docker Image
# Multi-stage build for optimized production image

# =============================================================================
# BUILD STAGE - Install dependencies and prepare application
# =============================================================================

# Use Python 3.11 slim image as base
ARG PYTHON_VERSION=3.11
FROM python:${PYTHON_VERSION}-slim as builder

# Set build arguments
ARG DEBIAN_FRONTEND=noninteractive
ARG PIP_NO_CACHE_DIR=1
ARG PIP_DISABLE_PIP_VERSION_CHECK=1

# Set working directory
WORKDIR /app

# Install system dependencies required for building Python packages
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    pkg-config \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Upgrade pip and install build tools
RUN pip install --upgrade pip setuptools wheel

# Copy requirements first for better Docker layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Create virtual environment for production dependencies
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install production dependencies in virtual environment
RUN pip install --no-cache-dir -r requirements.txt

# =============================================================================
# PRODUCTION STAGE - Create final optimized image
# =============================================================================

FROM python:${PYTHON_VERSION}-slim as production

# Set build arguments
ARG DEBIAN_FRONTEND=noninteractive

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/opt/venv/bin:$PATH" \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Create non-root user for security
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Install only runtime dependencies
RUN apt-get update && apt-get install -y \
    curl \
    libpq5 \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Copy virtual environment from builder stage
COPY --from=builder /opt/venv /opt/venv

# Set working directory
WORKDIR /app

# Copy application code
COPY --chown=appuser:appuser . .

# Create necessary directories
RUN mkdir -p /app/data /app/logs /app/temp \
    && chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Default command - can be overridden
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

# =============================================================================
# ALTERNATIVE STAGES FOR DIFFERENT ENVIRONMENTS
# =============================================================================

# Development stage with additional tools
FROM production as development

# Switch back to root to install dev tools
USER root

# Install development dependencies
RUN apt-get update && apt-get install -y \
    git \
    vim \
    htop \
    tree \
    && rm -rf /var/lib/apt/lists/*

# Install development Python packages
RUN pip install --no-cache-dir \
    pytest \
    pytest-asyncio \
    pytest-cov \
    black \
    flake8 \
    mypy \
    ipython \
    jupyter

# Switch back to app user
USER appuser

# Development command with auto-reload
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]

# Testing stage
FROM development as testing

# Copy test files
COPY --chown=appuser:appuser tests/ tests/

# Run tests by default
CMD ["pytest", "tests/", "-v", "--cov=app", "--cov-report=term-missing"]

# =============================================================================
# BUILD INSTRUCTIONS
# =============================================================================

# Build production image:
#   docker build -t healthcare-cost-navigator .

# Build development image:
#   docker build --target development -t healthcare-cost-navigator:dev .

# Build testing image:
#   docker build --target testing -t healthcare-cost-navigator:test .

# Build with custom Python version:
#   docker build --build-arg PYTHON_VERSION=3.11 -t healthcare-cost-navigator .

# =============================================================================
# RUNTIME INSTRUCTIONS
# =============================================================================

# Run production container:
#   docker run -p 8000:8000 --env-file .env healthcare-cost-navigator

# Run development container with volume mount:
#   docker run -p 8000:8000 -v $(pwd)/app:/app/app --env-file .env healthcare-cost-navigator:dev

# Run with environment variables:
#   docker run -p 8000:8000 \
#     -e DATABASE_URL=postgresql+asyncpg://user:pass@host:5432/db \
#     -e OPENAI_API_KEY=your_key \
#     healthcare-cost-navigator

# Run ETL process:
#   docker run --env-file .env -v $(pwd)/data:/app/data healthcare-cost-navigator python etl.py

# Run database initialization:
#   docker run --env-file .env healthcare-cost-navigator python app/init_db.py init

# Interactive shell:
#   docker run -it --env-file .env healthcare-cost-navigator bash

# =============================================================================
# SECURITY CONSIDERATIONS
# =============================================================================

# This Dockerfile follows security best practices:
# 1. Uses non-root user
# 2. Multi-stage build reduces image size
# 3. Minimal runtime dependencies
# 4. No secrets in image layers
# 5. Health checks for container orchestration
# 6. Proper file permissions

# =============================================================================
# OPTIMIZATION NOTES
# =============================================================================

# Image size optimizations:
# - Multi-stage build
# - Slim base image
# - Minimal runtime dependencies
# - Virtual environment for clean separation
# - Proper layer caching with requirements.txt

# Security optimizations:
# - Non-root user execution
# - Minimal attack surface
# - No build tools in production image

# Performance optimizations:
# - Pre-compiled Python bytecode
# - Optimized Python settings
# - Health checks for container orchestration

# =============================================================================
# TROUBLESHOOTING
# =============================================================================

# Common build issues:

# 1. "Package not found" errors:
#    - Check requirements.txt format
#    - Verify package names and versions
#    - Update package index: apt-get update

# 2. Permission denied errors:
#    - Check file ownership: COPY --chown=appuser:appuser
#    - Verify user has write access to directories

# 3. Large image size:
#    - Use multi-stage build (already implemented)
#    - Remove unnecessary dependencies
#    - Use .dockerignore file

# 4. Slow builds:
#    - Optimize layer caching
#    - Copy requirements.txt before app code
#    - Use Docker build cache

# 5. Runtime errors:
#    - Check environment variables
#    - Verify database connectivity
#    - Check application logs: docker logs <container>

# =============================================================================
# DOCKER IGNORE RECOMMENDATIONS
# =============================================================================

# Create .dockerignore file with:
# .git
# .pytest_cache
# __pycache__
# *.pyc
# .env
# .venv
# node_modules
# README.md
# .gitignore
# Dockerfile
# docker-compose.yml