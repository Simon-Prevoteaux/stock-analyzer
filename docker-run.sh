#!/bin/bash

# Stock Analyzer - Docker Run Script
# This script runs the Docker container with the Stock Analyzer application

set -e  # Exit on error

# Default port
PORT=5001

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -p|--port)
            PORT="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [-p|--port PORT]"
            exit 1
            ;;
    esac
done

echo "======================================"
echo "Stock Analyzer - Docker Run"
echo "======================================"
echo ""

# Check if image exists
if ! docker image inspect stock-analyzer:latest > /dev/null 2>&1; then
    echo "Error: Docker image 'stock-analyzer:latest' not found."
    echo "Please build the image first using:"
    echo "  ./docker-build.sh"
    exit 1
fi

# Stop and remove existing container if running
if docker ps -a --format '{{.Names}}' | grep -q '^stock-analyzer$'; then
    echo "Stopping existing container..."
    docker stop stock-analyzer > /dev/null 2>&1 || true
    docker rm stock-analyzer > /dev/null 2>&1 || true
fi

# Run the container
echo "Starting Stock Analyzer container..."
docker run -d \
    --name stock-analyzer \
    -p ${PORT}:5000 \
    -v "$(pwd)/data:/app/data" \
    -e PORT=5000 \
    --restart unless-stopped \
    stock-analyzer:latest

echo ""
echo "======================================"
echo "Container started successfully!"
echo "======================================"
echo ""
echo "Container name: stock-analyzer"
echo "Access the application at: http://localhost:${PORT}"
echo ""
echo "Useful commands:"
echo "  View logs:     docker logs -f stock-analyzer"
echo "  Stop:          docker stop stock-analyzer"
echo "  Restart:       docker restart stock-analyzer"
echo "  Remove:        docker rm -f stock-analyzer"
echo ""
