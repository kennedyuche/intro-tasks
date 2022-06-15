FROM python:3.9-alpine

RUN mkdir app

COPY split_csv.py /app/split_csv.py

COPY test_data.csv /app

RUN cd /app

WORKDIR /app

ENTRYPOINT ["python", "split_csv.py", "-i", "test_data.csv", "-o", "output", "-r"]

CMD [ "100" ]