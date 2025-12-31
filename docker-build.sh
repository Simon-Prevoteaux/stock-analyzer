#!/bin/bash

# Stock Analyzer - Docker Build Script
# This script builds the Docker image for the Stock Analyzer application

set -e  # Exit on error

echo "======================================"
echo "Stock Analyzer - Docker Build"
echo "======================================"
echo ""

# Build the Docker image
echo "Building Docker image..."
docker build -t stock-analyzer:latest .

echo ""
echo "======================================"
echo "Build completed successfully!"
echo "======================================"
echo ""
echo "Image: stock-analyzer:latest"
echo ""
echo "To run the container, use:"
echo "  ./docker-run.sh"
echo ""
echo "Or use docker-compose:"
echo "  docker-compose up"
echo ""
