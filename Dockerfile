FROM python:3.13-slim

WORKDIR /app

# Install dependencies
COPY requirements-prefect.txt .
RUN pip install --no-cache-dir -r requirements-prefect.txt

# Copy application code
COPY api/ api/
COPY collectors/ collectors/
COPY config/ config/
COPY db/ db/
COPY flows/ flows/
COPY utils/ utils/
COPY main_ultrafast.py .
COPY collect_metadata.py .
COPY prefect.yaml .

# Create logs directory
RUN mkdir -p logs

ENV PYTHONUNBUFFERED=1
