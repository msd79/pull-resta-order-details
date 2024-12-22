FROM python:3.11-slim

WORKDIR /app

# Install system dependencies in a single layer
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        gnupg2 \
        unixodbc \
        unixodbc-dev \
        software-properties-common \
        gcc \
        g++ \
        && \
    curl https://packages.microsoft.com/keys/microsoft.asc | tee /etc/apt/trusted.gpg.d/microsoft.asc && \
    curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list | tee /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        msodbcsql17 \
        msodbcsql18 \
        && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


RUN apt-get update && apt-get install -y \
iputils-ping \
netcat-traditional \
&& rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Verify ODBC drivers are installed
RUN odbcinst -q -d

# Ensure log directory exists
RUN mkdir -p /app/order_sync.log

# Environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]