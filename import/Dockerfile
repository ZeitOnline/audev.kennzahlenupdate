FROM python:3.7
WORKDIR /main-app

# Install python dependencies
COPY requirements.txt .
RUN pip install --no-deps -r requirements.txt

COPY import/main.py .
COPY src ./src

CMD python /main-app/main.py

