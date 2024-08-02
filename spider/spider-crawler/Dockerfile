FROM docker.io/python:3.10.0-alpine
COPY . .
RUN pip3 install -r requirements.txt
CMD ["python3", "-u", "crawler.py"]