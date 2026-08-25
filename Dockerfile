FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    LIANGJIAN_ROOT=/app

WORKDIR /app
COPY . /app
RUN python -m pip install --no-cache-dir .

VOLUME ["/app/state", "/app/storage", "/app/outputs", "/app/cache"]
ENTRYPOINT ["liangjian-funnel"]
CMD ["status"]
