# Этап 1: Сборка Python-пакета
FROM python:3.13-slim AS builder

WORKDIR /build
COPY . .

# Установка зависимостей для сборки
RUN pip install --no-cache-dir build wheel

# Сборка пакета
RUN python -m build

# Этап 2: Среда выполнения
FROM python:3.13-slim

# Установка системных зависимостей
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Копирование собранного пакета из этапа сборки
COPY --from=builder /build/dist/*.whl /tmp/

# Установка пакета и его зависимостей
RUN pip install --no-cache-dir /tmp/*.whl \
    && rm -rf /tmp/*.whl

# Создание пользователя без прав root
RUN useradd -m -U app \
    && chown -R app:app /app

USER app

RUN mkdir -p /app/logs \
    && chown -R app:app /app/logs

# Запуск приложения
CMD ["python", "-m", "databaser.manage"]