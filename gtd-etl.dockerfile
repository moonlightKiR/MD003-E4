FROM python:3.11-slim

WORKDIR /gtd-etl

# Instalamos dependencias del sistema incluyendo JAVA para PySpark
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libffi-dev \
    libssl-dev \
    git \
    default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir /tmp/repo && \
    cd /tmp/repo && \
    git init && \
    git remote add -f origin https://github.com/moonlightKiR/MD003-E4.git && \
    git config core.sparseCheckout true && \
    echo "gtd-etl/" >> .git/info/sparse-checkout && \
    git pull origin main && \
    mv gtd-etl/* /gtd-etl/ && \
    cd /gtd-etl && \
    rm -rf /tmp/repo

RUN pip install --no-cache-dir -r requirements.txt