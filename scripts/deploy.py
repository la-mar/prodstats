"""
Example docker deployment to AWS ECS cluster.

The script does the following:

    1. Loads environment variables from .env file in the project root

    For each service in SERVICES
    2. Generates a populated ECS task definition
        - You can configure your task definitions in the get_task_definition() method.
    3. Optionally authenticate Docker to ECR
    4. Optionally build any configured containers
    5. Optionally push any configured containers to ECR
    6. Register the new task definition in ECR
    7. Retrieve the latest task definition revision number
    8. Update the running service with the new task definition
        and force a new deployment
"""

import os
from typing import List

import boto3
import tomlkit
from dotenv import dotenv_values


def get_project_meta() -> dict:
    pyproj_path = "./pyproject.toml"
    if os.path.exists(pyproj_path):
        with open(pyproj_path, "r") as pyproject:
            file_contents = pyproject.read()
        return tomlkit.parse(file_contents)["tool"]["poetry"]
    else:
        return {}


pkg_meta = get_project_meta()
project = pkg_meta.get("name")
version = pkg_meta.get("version")

ENV = os.getenv("ENV", "prod")
AWS_ACCOUNT_ID = os.getenv("AWS_ACCOUNT_ID")
SERVICE_NAME: str = os.getenv("SERVICE_NAME")  # type: ignore
IMAGE_TAG: str = os.getenv("IMAGE_TAG")  # type: ignore
IMAGE_NAME: str = f"{os.getenv('IMAGE_NAME')}{':' if IMAGE_TAG else ''}{IMAGE_TAG or ''}"  # noqa

CLUSTER_NAME = os.getenv("ECS_CLUSTER")  # type: ignore
TASK_IAM_ROLE = f"arn:aws:iam::{AWS_ACCOUNT_ID}:role/prodstats-task-role"

if not any([ENV, AWS_ACCOUNT_ID, SERVICE_NAME, IMAGE_NAME, CLUSTER_NAME]):
    raise ValueError("One or more environment variables are missing")


SERVICES: List[str] = [
    "prodstats-web",
    "prodstats-worker-collector",
    "prodstats-worker-deleter",
    "prodstats-worker-submitter",
    "prodstats-worker-default",
    "prodstats-cron",
]

IMAGES = [
    {"name": SERVICE_NAME, "dockerfile": "Dockerfile", "build_context": "."},
]

TAGS = [
    {"key": "domain", "value": "technology"},
    {"key": "service_name", "value": project},
    {"key": "environment", "value": ENV},
    {"key": "terraform", "value": "true"},
]


BUILD = False
PUSH = False

print("\n\n" + "-" * 30)
print(f"ENV: {ENV}")
print(f"AWS_ACCOUNT_ID: {AWS_ACCOUNT_ID}")
print(f"CLUSTER_NAME: {CLUSTER_NAME}")
print(f"SERVICES: {SERVICES}")
print("-" * 30 + "\n\n")


task_envs = dotenv_values(".env.production")


def transform_envs(d: dict):
    return [{"name": k, "value": v} for k, v in d.items()]


def get_task_definition(
    name: str,
    envs: dict,
    service_name: str,
    tags: list = [],
    task_iam_role_arn: str = "ecsTaskExecutionRole",
):
    image = IMAGE_NAME
    defs = {
        "prodstats-web": {
            "containerDefinitions": [
                {
                    "name": "prodstats-web",
                    "command": [
                        "prodstats",
                        "run",
                        "web",
                        "-b 0.0.0.0:9090",
                        "--statsd-host=localhost:8125",
                    ],
                    "memoryReservation": 128,
                    "cpu": 256,
                    "image": image,
                    "essential": True,
                    "environment": transform_envs(envs),
                    "portMappings": [
                        {"hostPort": 9090, "containerPort": 9090, "protocol": "tcp"}
                    ],
                },
            ],
            "executionRoleArn": "ecsTaskExecutionRole",
            "family": f"{service_name}",
            "networkMode": "awsvpc",
            "taskRoleArn": task_iam_role_arn,
            "tags": tags,
        },
        "prodstats-worker-submitter": {
            "containerDefinitions": [
                {
                    "name": "prodstats-worker",
                    "command": [
                        "prodstats",
                        "run",
                        "worker",
                        "-c",
                        "10",
                        "-Q",
                        "prodstats-submissions-h,prodstats-submissions-v",
                        # "--loglevel",
                        # "info",
                    ],
                    "memoryReservation": 128,
                    "cpu": 256,
                    "image": image,
                    "essential": True,
                    "environment": transform_envs(envs),
                },
            ],
            "executionRoleArn": "ecsTaskExecutionRole",
            "family": f"{service_name}",
            "networkMode": "bridge",
            "taskRoleArn": task_iam_role_arn,
            "tags": tags,
        },
        "prodstats-worker-collector": {
            "containerDefinitions": [
                {
                    "name": "prodstats-worker",
                    "command": [
                        "prodstats",
                        "run",
                        "worker",
                        "-c",
                        "10",
                        "-Q",
                        "prodstats-collections-h,prodstats-collections-v",
                        # "--loglevel",
                        # "warn",
                    ],
                    "memoryReservation": 512,
                    "cpu": 512,
                    "image": image,
                    "essential": True,
                    "environment": transform_envs(envs),
                },
            ],
            "executionRoleArn": "ecsTaskExecutionRole",
            "family": f"{service_name}",
            "networkMode": "bridge",
            "taskRoleArn": task_iam_role_arn,
            "tags": tags,
        },
        "prodstats-worker-deleter": {
            "containerDefinitions": [
                {
                    "name": "prodstats-worker",
                    "command": [
                        "prodstats",
                        "run",
                        "worker",
                        "-c",
                        "10",
                        "-Q",
                        "prodstats-deletions-h,prodstats-deletions-v",
                        # "--loglevel",
                        # "warn",
                    ],
                    "memoryReservation": 512,
                    "cpu": 512,
                    "image": image,
                    "essential": True,
                    "environment": transform_envs(envs),
                },
            ],
            "executionRoleArn": "ecsTaskExecutionRole",
            "family": f"{service_name}",
            "networkMode": "bridge",
            "taskRoleArn": task_iam_role_arn,
            "tags": tags,
        },
        "prodstats-worker-default": {
            "containerDefinitions": [
                {
                    "name": "prodstats-worker",
                    "command": [
                        "prodstats",
                        "run",
                        "worker",
                        "-c",
                        "10",
                        "-Q",
                        "prodstats-default",
                        # "--loglevel",
                        # "warn",
                    ],
                    "memoryReservation": 256,
                    "cpu": 512,
                    "image": image,
                    "essential": True,
                    "environment": transform_envs(envs),
                },
            ],
            "executionRoleArn": "ecsTaskExecutionRole",
            "family": f"{service_name}",
            "networkMode": "bridge",
            "taskRoleArn": task_iam_role_arn,
            "tags": tags,
        },
        "prodstats-cron": {
            "containerDefinitions": [
                {
                    "name": "prodstats-cron",
                    "command": ["prodstats", "run", "cron", "--loglevel", "debug"],
                    "memoryReservation": 256,
                    "cpu": 512,
                    "image": image,
                    "essential": True,
                    "environment": transform_envs(envs),
                },
            ],
            "executionRoleArn": "ecsTaskExecutionRole",
            "family": f"{service_name}",
            "networkMode": "bridge",
            "taskRoleArn": task_iam_role_arn,
            "tags": tags,
        },
    }

    return defs[name]


class AWSClient:
    access_key_id = None
    secret_access_key = None
    session_token = None
    account_id = None
    region = None
    _ecs = None

    def __init__(self):
        self.credentials()

    @property
    def has_credentials(self):
        return all(
            [
                self.access_key_id is not None,
                self.secret_access_key is not None,
                self.region is not None,
                self.account_id is not None,
            ]
        )

    @property
    def ecr_url(self):
        if not self.has_credentials:
            self.credentials()
        return f"{self.account_id}.dkr.ecr.{self.region}.amazonaws.com"

    def credentials(self):
        credentials = {
            "access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
            "secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
            "region": os.getenv("AWS_REGION", "us-east-1"),
            "account_id": os.getenv("AWS_ACCOUNT_ID"),
            "session_token": os.getenv("AWS_SESSION_TOKEN"),
            "security_token": os.getenv("AWS_SECURITY_TOKEN"),
        }
        [setattr(self, k, v) for k, v in credentials.items()]  # type: ignore

        return credentials

    def get_client(self, service_name: str):

        if not self.has_credentials:
            self.credentials()

        return boto3.client(
            service_name,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            region_name=self.region,
            aws_session_token=self.session_token,
        )

    @property
    def ecs(self):
        return self._ecs or self.get_client("ecs")

    def get_latest_revision(self, task_name: str):
        response = self.ecs.describe_task_definition(taskDefinition=task_name)
        return response["taskDefinition"]["revision"]


client = AWSClient()


results = []


for service in SERVICES:
    s = f"{service:>20}:"
    prev_rev_num = client.get_latest_revision(service)
    cdef = get_task_definition(
        name=service,
        envs=task_envs,
        service_name=service,
        tags=TAGS,
        task_iam_role_arn=TASK_IAM_ROLE,
    )

    # pprint(cdef)
    client.ecs.register_task_definition(**cdef)

    rev_num = client.get_latest_revision(service)
    s += "\t" + f"updated revision: {prev_rev_num} -> {rev_num}"
    results.append((service, prev_rev_num, rev_num))
    print(s)

for service, prev_rev_num, rev_num in results:
    response = client.ecs.update_service(
        cluster=CLUSTER_NAME,
        service=service,
        forceNewDeployment=True,
        taskDefinition=f"{service}:{rev_num}",
    )
    print(f"{service:>20}: updated service on cluster {CLUSTER_NAME}")
print("\n\n")
