FROM python:3.11-slim

WORKDIR /app

# Installer les dépendances
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY simulator.py .

CMD ["python", "-u", "simulator.py"]
