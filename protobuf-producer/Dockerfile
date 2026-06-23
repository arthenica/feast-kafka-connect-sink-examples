FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY taxi_trip.proto protobuf_producer.py ./

# Generate taxi_trip_pb2.py from the .proto (protoc is provided by grpcio-tools).
RUN python -m grpc_tools.protoc -I. --python_out=. taxi_trip.proto

CMD ["python", "protobuf_producer.py"]
