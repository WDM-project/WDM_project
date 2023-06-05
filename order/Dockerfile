FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /home/flask-app

COPY ./requirements.txt .

RUN pip install -r requirements.txt

RUN apt-get update && \
    apt-get install -y curl

RUN apt-get update && \
apt-get install -y curl netcat  # Added netcat here

COPY . .

RUN chmod +x wait-for-kafka.sh

EXPOSE 5000
CMD ["./wait-for-kafka.sh", "python", "-u", "app.py"]