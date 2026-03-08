FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt requirements.txt
COPY etl_framework.py .
COPY transforms/ ./transforms/

# Install AWS CLI
RUN apt-get update && \
    apt-get install -y awscli && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir -r requirements.txt

# Azure configuration
ENV AZURE_ACCOUNT=your_account
ENV AZURE_SAS_TOKEN="?sv=..."

# AWS configuration
ENV AWS_ACCESS_KEY=AKIA...
ENV AWS_SECRET_KEY=...

CMD ["python", "src/etl_framework.py", "--config", "/path/to/config.yaml"]
