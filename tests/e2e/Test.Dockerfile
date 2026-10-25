FROM python:3.12-slim
WORKDIR /tests
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY producers/ ./producers/
RUN python -m grpc_tools.protoc -Iproducers --python_out=producers producers/taxi_trip.proto
COPY config/ ./config/
COPY run.py fixtures.py ./
ENTRYPOINT ["python", "-u", "run.py"]
