# One image for both processes; docker-compose.yml picks the command:
#   migrate: alembic upgrade head          (once, before the others start)
#   api:     python -m binance_connector.server   (waitress, port 5000)
#   worker:  python -m binance_connector.worker   (execution, streams, WebSocket on 8765)
FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Dependencies and the package. The source stays in /app too: alembic.ini points at the
# migrations folder by path.
COPY pyproject.toml README.md ./
COPY binance_connector ./binance_connector
RUN pip install . && rm -rf build *.egg-info
COPY alembic.ini ./

# Run as an unprivileged user; the SQLite database lives on the /data volume.
RUN useradd --create-home --uid 10001 service \
    && mkdir /data \
    && chown service /data
USER service

# Inside a container the services must listen on every interface to be reachable at all;
# docker-compose.yml publishes them on the host's 127.0.0.1 only.
ENV DATABASE_URL=sqlite:////data/binance_connector.db \
    API_HOST=0.0.0.0 \
    WEBSOCKET_HOST=0.0.0.0

EXPOSE 5000 8765
CMD ["python", "-m", "binance_connector.server"]
