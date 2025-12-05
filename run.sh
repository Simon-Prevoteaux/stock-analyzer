#!/bin/bash

# Stock Analyzer - Run Script

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

# Navigate to webapp directory
cd src/webapp

# Run Flask app
echo "Starting Flask application on http://localhost:5000"
python app.py
