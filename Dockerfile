FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# tzdata: necesario para zoneinfo (America/Puerto_Rico)
RUN apt-get update \
    && apt-get install -y --no-install-recommends tzdata \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY core ./core
COPY run_reconnect.py .

RUN useradd --create-home --uid 10001 appuser
USER appuser

# Una ejecución diaria a la hora CONNEX_DAILY_AT (ver docker-compose.yml).
CMD ["python", "run_reconnect.py"]
