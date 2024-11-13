

## to login to DB
sqlite3 jobs.db

## add a new row to table jobs and test
INSERT INTO jobs (creation_time, status, parameters, scanner_host_name)
VALUES ('2024-11-12 10:00:00', 'QUEUED', 'param1,param2', 'scanner1');

## Run a test
python ./orchestrator/app.py

```
Found 1 available jobs.
Started Fargate task: arn:aws:ecs:us-west-2:602843233966:task/my-cluster/32b132d318a94c1ebf0f5c97aedd2a97
Found 0 available jobs.
```