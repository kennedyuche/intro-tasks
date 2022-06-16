FROM python:latest

WORKDIR /docker_task2

ENV PORT 80

COPY split_csv.py ./docker_task2

ADD test_data.csv ./docker_task2


COPY . /docker_task2/

#CMD ["python", "split_csv.py", "test_data.csv"]

ENTRYPOINT [ "python", "split_csv.py", "-i test_data.csv -o test_data.csv -r 100" ]

