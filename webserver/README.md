# Projet SDTD

FastAPI webserver for the SDTD project. 

## Environment variables
```bash
cp .env.example .env # Copy example environment variables
```

## Run locally
Python virtual environment is recommended to avoid conflicts with other projects :
```bash
python3 -m venv sdtd-webserver # Create virtual environment
source sdtd-webserver/bin/activate # Activate virtual environment
```

Install dependencies and run the server :
```bash
pip install -r requirements.txt # Install dependencies
uvicorn app.main:app --reload # Run server
```

## Run in a container
```bash
docker build -t sdtd-webserver-image . # Build image
docker run -d --name sdtd-webserver-container -p 8000:80 sdtd-webserver-image # Run container
```

## Docs
Docs are available at http://127.0.0.1:8000/docs
