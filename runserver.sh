#!/bin/bash
source .venv/bin/activate

# Kill any process using port 8000
PID=$(lsof -ti:8000)
if [ -n "$PID" ]; then
  echo "Killing existing process on port 8000 (PID $PID)"
  kill -9 $PID
fi

uvicorn src.web.asgi:asgi_app --host 0.0.0.0 --port 8000 --reload
# Note: Adjust the host IP as necessary for your environment.
# To run this script, ensure it has execute permissions:
# chmod +x runserver.sh
# You can then run it with:
# ./runserver.sh
# Ensure the script is run from the project root directory
# where the .venv directory is located.
# This script activates the virtual environment, kills any existing process on port 8000,
# and starts the Uvicorn server with the specified host and port.
# Ensure you have the necessary permissions to kill processes and bind to the specified port.
# If you want to run this script in the background, you can use: