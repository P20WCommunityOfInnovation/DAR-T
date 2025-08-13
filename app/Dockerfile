FROM python:3.10-slim

WORKDIR /app

COPY . .

RUN apt-get update && apt-get install -y curl && pip3 install --no-cache-dir -r requirements.txt --upgrade --prefer-binary -i https://pypi.org/simple
COPY config.toml /root/.streamlit/config.toml

# Environment variables for DART
# This env variables means  no need to show the 3 dots menu on the top right corner
#ENV DISABLE_STREAMLIT_HAMBURGER=true

EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

ENTRYPOINT ["streamlit", "run", "dart_app.py", "--server.port=8501", "--server.address=0.0.0.0"]