FROM python:3.12-slim

WORKDIR /app

# Dependencies first so code changes don't invalidate this layer.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY collector/ .

CMD ["python", "poller.py"]
