FROM python:3.7
WORKDIR /fc-app

# Install python dependencies
COPY requirements.txt .
RUN pip install --no-deps -r requirements.txt

COPY forecast/main_fc.py .
COPY src ./src

CMD python /fc-app/main_fc.py

