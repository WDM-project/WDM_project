FROM python:3.11-slim

WORKDIR /home/flask-app

COPY ./requirements.txt .

COPY wait-for-kafka.sh /wait-for-kafka.sh


RUN pip install -r requirements.txt

RUN apt-get update && \
    apt-get install -y curl netcat  # Added netcat here
    
COPY . .

RUN chmod +x wait-for-kafka.sh

