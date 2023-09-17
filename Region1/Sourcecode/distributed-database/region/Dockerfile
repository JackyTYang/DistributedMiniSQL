# syntax=docker/dockerfile:1
FROM python:3.9-alpine
WORKDIR /minisql
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY ./src ./src
CMD ["python", "-u", "./src/main.py", "default"]