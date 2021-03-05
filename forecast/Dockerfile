FROM python:3.7
WORKDIR /fc-app

# Install python dependencies
COPY ../requirements.txt .
RUN pip install --no-deps -r requirements.txt

COPY ../src ./src
COPY main_fc.py .
COPY ../credentials/audev-bigquery-default.json .
COPY ../credentials/config.json .
COPY ../credentials/admanager-auth.json .

CMD python /fc-app/main_fc.py

