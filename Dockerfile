FROM python:3.8

WORKDIR /pro
COPY Airline_Delay_Cause.csv source.csv
COPY pipeline.py pipeline_c.py
RUN pip install pandas

ENTRYPOINT [ "python", "pipeline_c.py" ]
