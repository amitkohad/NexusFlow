FROM python:3.12-slim AS build
WORKDIR /build
COPY requirements.txt .
RUN python -m venv /opt/venv && /opt/venv/bin/pip install --no-cache-dir -r requirements.txt

FROM python:3.12-slim
RUN useradd --create-home --uid 10001 nexusflow
COPY --from=build /opt/venv /opt/venv
WORKDIR /app
COPY --chown=nexusflow:nexusflow app app
COPY --chown=nexusflow:nexusflow examples examples
ENV PATH="/opt/venv/bin:$PATH" PYTHONUNBUFFERED=1
USER nexusflow
CMD ["python", "-m", "app.worker"]
