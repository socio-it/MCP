FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

WORKDIR /app

RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copia archivos de dependencias
COPY pyproject.toml uv.lock* ./

# Instala dependencias
RUN uv sync --frozen --no-cache --no-dev

# Copia todo el código, incluyendo .env y otros nuevos
COPY . .

EXPOSE 3000

CMD ["uv", "run", "python", "main.py"]
