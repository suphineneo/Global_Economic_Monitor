FROM python:3.9

WORKDIR /app 

COPY requirements.txt .

RUN pip install -r requirements.txt 

COPY /app .

CMD ["python", "-m", "etl_project.pipelines.global_economic_monitor"]
