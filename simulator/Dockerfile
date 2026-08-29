FROM python:3.11-slim

WORKDIR /app
COPY simulator.py .

# No external dependencies — uses only stdlib
ENV PYTHONUNBUFFERED=1

CMD ["python", "simulator.py"]