FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

# Copy requirements first for better caching

COPY requirements.txt .

# Install dependencies

RUN pip install --no-cache-dir --upgrade pip setuptools wheel && 
pip install --no-cache-dir -r requirements.txt

# Copy all project files

COPY . .

# Expose Flask/Koyeb port

EXPOSE 8000

# Run NIFTY bot

CMD ["python", "-u", "main.py"]
