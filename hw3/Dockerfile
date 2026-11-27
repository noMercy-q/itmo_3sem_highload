FROM python:3.13-slim-bookworm
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src ./src
COPY tests ./tests
ENV PYTHONPATH=/app/src PYTHONUNBUFFERED=1
RUN mkdir /data && chown 1000:1000 /data
USER 1000:1000
ENTRYPOINT ["python", "-m", "queue_lab"]
