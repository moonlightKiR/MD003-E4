FROM python:3.11-alpine

WORKDIR /gtd-etl

RUN apk add --no-cache \
    build-base \
    libffi-dev \
    openssl-dev \
    git

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