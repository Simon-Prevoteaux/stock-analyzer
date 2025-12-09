#!/bin/bash

# Stock Analyzer - Run Script

# Parse command line arguments
PORT=5000  # Default port

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

echo "Starting Stock Analyzer..."

# Activate virtual environment
if [ -d "venv" ]; then
    source venv/bin/activate
    echo "Virtual environment activated"
else
    echo "Error: Virtual environment not found. Please run setup first."
    exit 1
fi

# Create data directory if it doesn't exist
mkdir -p data

# Export PORT environment variable
export PORT=$PORT

# Navigate to webapp directory
cd src/webapp

# Run Flask app
echo "Starting Flask application on http://localhost:$PORT"
python app.py
