FROM python:3.12-slim
RUN pip install --no-cache-dir kafka-python==2.2.15
WORKDIR /app
ENV PYTHONPATH=/app PYTHONUNBUFFERED=1
RUN useradd --uid 10001 --create-home hospital && mkdir /state && chown hospital /state
USER hospital
CMD ["python", "-m", "dashboard.hospital.server"]
