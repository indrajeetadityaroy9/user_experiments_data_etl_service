#!/bin/bash

# Function to start Docker application
function start_docker_app() {
    docker-compose up --build -d
}

# Function to provide user instructions
function provide_user_instructions() {
    echo ""
    echo "====================================="
    echo "  USER EXPERIMENTS DATA ETL SERVICE"
    echo "====================================="
    echo ""
    echo "To upload csv data to DB, via an http client(PostMan, Insomnia, Curl etc) issue a POST request to: http://127.0.0.1:5001/trigger"
    echo "To query DB, input instruction: query"
    echo "To exit service, input instruction: exit"
    echo ""
}

# Function that queries the database
function query_database() {
    docker exec etl-app python query.py
}

# Main script
start_docker_app
provide_user_instructions

read -p "Enter instruction: " command

# Check if the command is "query"
while [[ $command == "query" ]]; do
    # Call DB query funcition
    output=$(query_database)
    
    # Query output filtering
    if [[ $output == *"Warning"* ]]; then
        echo "Warning: The table is empty."
    else
        echo "$output"
    fi
    
    read -p "Enter instruction: " command
done


