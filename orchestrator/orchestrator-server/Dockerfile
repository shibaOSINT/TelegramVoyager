FROM docker.io/python:3.10.0-alpine
COPY ./shared .
COPY ./orchestrator/orchestrator-server .
RUN ["chmod", "+x", "/diagnostics.py"]
WORKDIR /usr/local/bin/
RUN ["ln", "-s", "/diagnostics.py", "diag"]
WORKDIR /
RUN pip3 install -r requirements.txt
CMD ["python3", "-u", "server.py"]