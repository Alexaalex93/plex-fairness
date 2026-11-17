FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY collector.py .
# -u so logs show up right away in the Unraid docker log
CMD ["python", "-u", "collector.py"]
