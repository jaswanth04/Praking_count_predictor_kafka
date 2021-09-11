FROM python:3.6-slim

RUN pip3 install flask
RUN pip3 install kafka_python

WORKDIR /app

COPY content /app/
COPY templates /app/templates
COPY app.py /app/

EXPOSE 4000


CMD ["python3", "app.py"]
