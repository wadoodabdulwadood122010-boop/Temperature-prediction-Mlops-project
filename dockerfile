FROM python:3.9-slim

WORKDIR /flask_app

# Install compilers (Fixes the "GCC" error for Pandas/XGBoost)
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY app/req.txt .

RUN pip install --upgrade pip && \
    pip install --default-timeout=1000 --no-cache-dir -r req.txt

COPY app/ .

EXPOSE 5000

CMD ["python", "app.py"]