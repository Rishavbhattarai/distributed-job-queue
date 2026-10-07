FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install ".[server]"
COPY alembic.ini ./
COPY migrations ./migrations

RUN useradd --create-home --uid 10001 jobq
USER jobq

EXPOSE 8000
# Overridden per service in docker-compose.yml (api / worker / migrate).
CMD ["uvicorn", "jobq.api:app", "--host", "0.0.0.0", "--port", "8000"]
