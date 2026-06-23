# Feast feature server for the demo: applies the NYC-taxi feature repo and serves the HTTP API
# (including POST /push and POST /get-online-features) on port 6566.
FROM python:3.12-slim

WORKDIR /feature_repo

RUN pip install --no-cache-dir "feast==0.61.0" "protobuf<6" "websockets<14" "uvicorn-worker"

COPY feature_store.yaml features.py seed_data.py ./

# Seed parquets matching the schemas so the FileSource paths exist for offline pushes,
# then register the feature definitions.
RUN mkdir -p data && python seed_data.py && feast apply

EXPOSE 6566

CMD ["feast", "serve", "-h", "0.0.0.0", "-p", "6566"]
