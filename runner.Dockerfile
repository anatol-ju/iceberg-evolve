FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y openjdk-17-jdk

# Install Poetry
RUN pip install poetry && \
    poetry config virtualenvs.in-project true && \
    poetry config cache-dir /var/cache/pypoetry && \
    poetry config installer.parallel true

# Copy only dependency metadata
COPY pyproject.toml poetry.lock README.md /app/

# Install dependencies (cached unless these files change)
RUN poetry install --no-root --all-extras --no-interaction --no-ansi

# Copy the rest of the project (triggers rebuild only if these files change)
COPY . /app

# Install the actual project (fast, uncached if source changes)
RUN poetry install --only-root --no-interaction --no-ansi

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
