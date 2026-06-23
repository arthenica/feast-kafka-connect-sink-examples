FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY avro_producer.py ./

CMD ["python", "avro_producer.py"]
