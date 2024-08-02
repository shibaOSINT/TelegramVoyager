FROM docker.io/python:3.10.0-alpine
COPY ./spider/spider-reporter .
COPY ./shared .
RUN pip3 install -r requirements.txt
CMD ["python3", "-u", "reporter.py"]
