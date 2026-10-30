FROM python:3.11-slim-bookworm
RUN pip install --no-cache-dir kafka-python==2.0.2 streamlit==1.39.0 deltalake==0.20.1 pandas==2.2.3 pyarrow==17.0.0
WORKDIR /app
