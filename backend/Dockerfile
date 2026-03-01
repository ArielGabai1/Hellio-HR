FROM python:3.14.2-alpine

WORKDIR /app

COPY requirements.txt .
RUN apk add --no-cache gcc musl-dev \
    && pip install --no-cache-dir -r requirements.txt

COPY main.py db.py ingest.py llm.py chat.py embeddings.py schema.sql pytest.ini ./
COPY prompts/ prompts/
COPY tests/ tests/

RUN mkdir -p /data/CVsJobs/cvs /data/CVsJobs/jobs /data/logs \
    && adduser -D -H hellio \
    && chown -R hellio /data
USER hellio

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
