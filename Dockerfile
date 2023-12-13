FROM python:3.11

WORKDIR /app
COPY src/ .

RUN pip install --no-cache-dir -r requirement.txt

CMD ["python", "./main.py"]
