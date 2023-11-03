FROM python:3.11

WORKDIR /app
COPY pipeline.py pipeline.py
RUN pip install pandas sqlalchemy psycopg2

ENTRYPOINT [ "python", "pipeline.py" ]