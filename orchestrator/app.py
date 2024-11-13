import boto3
import time
from sqlalchemy import create_engine, Column, Integer, String, DateTime, MetaData, Table, select, update
from datetime import datetime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import NoResultFound
import os

# AWS and Database configurations
DATABASE_URL = "sqlite:///jobs.db"  # SQLite database file
ECS_CLUSTER = "my-cluster"
ECS_TASK_DEFINITION = "my-task"
FARGATE_SUBNET = "subnet-0b74b21005d7991be"
FARGATE_SECURITY_GROUP = "sg-02df5dfd4847909e7"
MAX_WORKERS = 2  # Maximum number of Fargate workers to run in parallel
CHECK_INTERVAL = 30  # Polling interval in seconds

# Initialize AWS ECS client
ecs_client = boto3.client('ecs', region_name='us-west-2')

# Database setup
engine = create_engine(DATABASE_URL)
metadata = MetaData()
Session = sessionmaker(bind=engine)
session = Session()

# Define the jobs table structure (assuming it might not yet exist in SQLite)
jobs = Table(
    'jobs', metadata,
    Column('id', Integer, primary_key=True),
    Column('creation_time', DateTime, default=datetime.utcnow),
    Column('completion_time', DateTime, nullable=True),
    Column('status', String),
    Column('parameters', String),
    Column('scanner_host_name', String)
)

# Create the table if it doesn't exist
metadata.create_all(engine)

def get_available_jobs(session):
    """Retrieve jobs that are queued and ready to be processed."""
    query = select(jobs).where(jobs.c.status == "QUEUED")
    return session.execute(query).fetchall()

def start_fargate_task():
    """Start a new Fargate task."""
    response = ecs_client.run_task(
        cluster=ECS_CLUSTER,
        launchType="FARGATE",
        taskDefinition=ECS_TASK_DEFINITION,
        networkConfiguration={
            'awsvpcConfiguration': {
                'subnets': [FARGATE_SUBNET],
                'securityGroups': [FARGATE_SECURITY_GROUP],
                'assignPublicIp': "ENABLED"
            }
        }
    )
    task_arn = response["tasks"][0]["taskArn"]
    print(f"Started Fargate task: {task_arn}")
    return task_arn

def update_job_status(session, job_id, status, scanner_host_name):
    """Update job status and assign scanner host."""
    query = update(jobs).where(jobs.c.id == job_id).values(status=status, scanner_host_name=scanner_host_name)
    session.execute(query)
    session.commit()

def terminate_idle_workers(tasks):
    """Terminate any idle Fargate tasks."""
    for task_arn in tasks:
        response = ecs_client.describe_tasks(cluster=ECS_CLUSTER, tasks=[task_arn])
        for task in response["tasks"]:
            # Check if the task is idle based on application-specific logic
            if task["lastStatus"] == "STOPPED":
                print(f"Stopping idle Fargate task: {task_arn}")
                ecs_client.stop_task(cluster=ECS_CLUSTER, task=task_arn)
                tasks.remove(task_arn)

def main():
    running_tasks = []
    try:
        while True:
            # Step 1: Fetch available jobs
            available_jobs = get_available_jobs(session)
            print(f"Found {len(available_jobs)} available jobs.")

            # Step 2: Start new tasks if there are jobs and we haven't reached MAX_WORKERS
            for job in available_jobs:
                if len(running_tasks) < MAX_WORKERS:
                    task_arn = start_fargate_task()
                    scanner_host_name = task_arn.split("/")[-1]  # Using task ARN as unique scanner name
                    update_job_status(session, job.id, "AVAILABLE", scanner_host_name)
                    running_tasks.append(task_arn)
                else:
                    print("Max workers running; waiting for a task to complete.")
                    break

            # Step 3: Check for idle tasks and terminate them
            terminate_idle_workers(running_tasks)

            # Step 4: Wait for the next interval
            time.sleep(CHECK_INTERVAL)

    except KeyboardInterrupt:
        print("Orchestration script stopped by user.")
    finally:
        session.close()

if __name__ == "__main__":
    main()
