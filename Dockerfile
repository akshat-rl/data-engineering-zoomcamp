FROM prefecthq/prefect:2.7.7-python3.9

COPY docker_requirements.txt .

RUN pip install -r docker_requirements.txt

COPY week_2_workflow_orchestration/docker_test.py week_2_workflow_orchestration/docker_test.py

CMD ["python", "week_2_workflow_orchestration/docker_test.py"]

