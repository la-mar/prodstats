import os
import shutil
from typing import Dict, List, Optional

import boto3
import tomlkit
from colorama import Fore, init

init(autoreset=True)


def hr(frac: int = 1):
    return "-" * int(shutil.get_terminal_size().columns / frac) + "\n"


def get_project_meta() -> dict:
    pyproj_path = "./pyproject.toml"
    if os.path.exists(pyproj_path):
        with open(pyproj_path, "r") as pyproject:
            file_contents = pyproject.read()
        return tomlkit.parse(file_contents)["tool"]["poetry"]
    else:
        return {}


pkg_meta = get_project_meta()
project: str = pkg_meta.get("name")  # type: ignore
version: str = pkg_meta.get("version")  # type: ignore

if not project:
    raise ValueError("project name is missing")
if not version:
    raise ValueError("project version is missing")

ENV: Optional[str] = os.getenv("ENV")
AWS_ACCOUNT_ID: Optional[str] = os.getenv("AWS_ACCOUNT_ID")
AWS_REGION: str = os.getenv("AWS_REGION", "us-east-1")
SERVICE_NAME: Optional[str] = os.getenv("SERVICE_NAME", project)
IMAGE_TAG: Optional[str] = os.getenv("IMAGE_TAG", "latest")
IMAGE_NAME: str = os.getenv("IMAGE_NAME", project)
IMAGE = f"{IMAGE_NAME}:{IMAGE_TAG}"

TASK_IAM_ROLE = f"arn:aws:iam::{AWS_ACCOUNT_ID}:role/{project}-task-role"

IMAGES = [
    {"name": SERVICE_NAME, "dockerfile": "Dockerfile", "build_context": "."},
]

TAGS = [
    {"key": "domain", "value": "engineering"},
    {"key": "service_name", "value": project},
    {"key": "environment", "value": ENV},
    {"key": "terraform", "value": "true"},
]


SERVICES: List[Dict[str, Optional[str]]] = [
    {
        "task_name": f"{project}-web",
        "cluster_name": "ecs-web-cluster",
        "task_type": "service",  # service or scheduled
    },
    {
        "task_name": f"{project}-worker",
        "cluster_name": "ecs-collector-cluster",
        "task_type": "service",
    },
    {
        "task_name": f"{project}-cron",
        "cluster_name": "ecs-collector-cluster",
        "task_type": "service",
    },
    {
        "task_name": f"{project}-db-migrations",
        "cluster_name": None,
        "task_type": "service",
    },
]

TAGS = [
    {"key": "domain", "value": "engineering"},
    {"key": "service_name", "value": project},
    {"key": "environment", "value": ENV},
    {"key": "terraform", "value": "true"},
]


# EXAMPLE SCHEDULED TASK:
# "task_name_here": {
#     "service": "service_name_here",
#     "command": "container_command_here",
#     "rule": "cloudwatch_schedule_rule_name_here",
# },
SCHEDULED_TASKS: Dict[str, Dict] = {}


""" Print deployment summary """
tpl = "{name:>25} {value:<50}\n"
string = ""
string += tpl.format(name="ENV:", value=ENV)
string += tpl.format(name="AWS_ACCOUNT_ID:", value=AWS_ACCOUNT_ID)
string += tpl.format(name="AWS_REGION:", value=AWS_REGION)
string += tpl.format(name="SERVICE_NAME:", value=SERVICE_NAME)
string += tpl.format(name="IMAGE:", value=IMAGE)
print("\n\n" + hr(2) + string + hr(2))

if not all([ENV, AWS_ACCOUNT_ID, SERVICE_NAME, IMAGE, AWS_REGION]):
    raise ValueError("One or more environment variables are missing")


def get_task_definition(
    name: str,
    service_name: str,
    tags: list = [],
    task_iam_role_arn: str = "ecsTaskExecutionRole",
):
    image = IMAGE
    defs = {
        f"{project}-web": {
            "containerDefinitions": [
                {
                    "name": f"{project}-web",
                    "command": [
                        "chamber",
                        "exec",
                        f"{project}",
                        f"{project}-web",
                        "datadog",
                        "--",
                        f"{project}",
                        "run",
                        "web",
                        "--port",
                        "80",
                    ],
                    "memoryReservation": 256,
                    "cpu": 256,
                    "image": image,
                    "essential": True,
                    "portMappings": [
                        {"hostPort": 80, "containerPort": 80, "protocol": "tcp"}
                    ],
                },
            ],
            "executionRoleArn": "ecsTaskExecutionRole",
            "family": service_name,
            "networkMode": "awsvpc",
            "taskRoleArn": task_iam_role_arn,
            "tags": tags,
        },
        f"{project}-worker": {
            "containerDefinitions": [
                {
                    "name": f"{project}-worker",
                    "command": [
                        "chamber",
                        "exec",
                        f"{project}",
                        f"{project}-worker",
                        "datadog",
                        "--",
                        f"{project}",
                        "run",
                        "worker",
                        # "-Q",
                        # f"{project}-h,{project}-v",
                    ],
                    "memoryReservation": 3072,  # 1536,
                    "cpu": 1024,
                    "image": image,
                    "essential": True,
                    "user": "celeryuser",
                },
                # {
                #     "name": f"{project}-worker-default",
                #     "command": [
                #         "chamber",
                #         "exec",
                #         f"{project}",
                #         f"{project}-worker",
                #         "datadog",
                #         "--",
                #         f"{project}",
                #         "run",
                #         "worker",
                #         "-Q",
                #         f"{project}-default",
                #     ],
                #     "memoryReservation": 128,
                #     "cpu": 128,
                #     "image": image,
                #     "essential": True,
                #     "user": "celeryuser",
                # },
            ],
            "executionRoleArn": "ecsTaskExecutionRole",
            "family": service_name,
            "networkMode": "bridge",
            "taskRoleArn": task_iam_role_arn,
            "tags": tags,
        },
        f"{project}-cron": {
            "containerDefinitions": [
                {
                    "name": f"{project}-cron",
                    "command": [
                        "chamber",
                        "exec",
                        f"{project}",
                        f"{project}-cron",
                        "datadog",
                        "--",
                        f"{project}",
                        "run",
                        "cron",
                    ],
                    "memoryReservation": 160,
                    "cpu": 64,
                    "image": image,
                    "essential": True,
                    "user": "celeryuser",
                },
            ],
            "executionRoleArn": "ecsTaskExecutionRole",
            "family": service_name,
            "networkMode": "bridge",
            "taskRoleArn": task_iam_role_arn,
            "tags": tags,
        },
        f"{project}-db-migrations": {
            "containerDefinitions": [
                {
                    "name": f"{project}-db-migrations",
                    "command": [
                        "chamber",
                        "exec",
                        f"{project}",
                        "datadog",
                        "--",
                        f"{project}",
                        "db",
                        "upgrade",
                    ],
                    "memoryReservation": 128,
                    "cpu": 128,
                    "image": image,
                    "essential": True,
                },
            ],
            "executionRoleArn": "ecsTaskExecutionRole",
            "family": service_name,
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


results: List = []

events = client.get_client("events")
target_id = 0
targets = []


# * update ECS task definitions
for deployment in SERVICES:
    task_name = deployment["task_name"]

    if task_name:
        s = f"{Fore.GREEN}{task_name:>25}{Fore.RESET}"
        try:
            prev_rev_num = f"{Fore.GREEN}{client.get_latest_revision(task_name)}"
        except Exception:
            prev_rev_num = Fore.YELLOW + "?"
        cdef = get_task_definition(
            name=task_name,
            service_name=task_name,
            tags=TAGS,
            task_iam_role_arn=TASK_IAM_ROLE,
        )

        task_def_arn = client.ecs.register_task_definition(**cdef)["taskDefinition"][
            "taskDefinitionArn"
        ]

        rev_num = client.get_latest_revision(task_name)

        results.append(
            (
                task_name,
                deployment["task_type"],
                deployment["cluster_name"],
                prev_rev_num,
                rev_num,
                task_def_arn,
            )
        )
        print(
            f"{s}: updated revision ({prev_rev_num} {Fore.RESET}-> "
            + f"{Fore.GREEN}{rev_num}{Fore.RESET})"
        )
    else:
        print(f"{Fore.RED}No task_name specified: {deployment=}")


# * update ECS services and tasks scheduled through Cloudwatch
for task_name, task_type, cluster, prev_rev_num, rev_num, task_def_arn in results:
    if task_type == "service":
        try:
            if cluster:
                response = client.ecs.update_service(
                    cluster=cluster,
                    service=task_name,
                    forceNewDeployment=True,
                    taskDefinition=f"{task_name}:{rev_num}",
                )
                print(
                    f"{Fore.GREEN}{task_name:>25}{Fore.RESET}: updated service "
                    + f"on {Fore.GREEN}{cluster}"
                )
            else:
                print(
                    f"{Fore.GREEN}{task_name:>25}{Fore.RESET}: {Fore.YELLOW}SKIPPED{Fore.RESET} (no cluster specified)"  # noqa
                )
        except (
            client.ecs.exceptions.ServiceNotFoundException,
            client.ecs.exceptions.ServiceNotActiveException,
        ):
            print(
                f"{Fore.RED}{task_name:>25}{Fore.RESET}: "
                + f"{Fore.RED}NOT FOUND{Fore.RESET} on {cluster}"
            )

    elif task_type == "scheduled":
        if task_name:
            task_def = SCHEDULED_TASKS[task_name]
            service = task_def["service"]
            rule = task_def["rule"]
            task_count = 1
            cluster_arn = "arn:aws:ecs:{region}:{account_id}:cluster/{cluster_name}".format(
                region=AWS_REGION, account_id=AWS_ACCOUNT_ID, cluster_name=cluster
            )
            targets = [
                {
                    "Id": str(target_id),
                    "Arn": cluster_arn,
                    "RoleArn": f"arn:aws:iam::{AWS_ACCOUNT_ID}:role/ecsEventsRole",
                    "EcsParameters": {
                        "TaskDefinitionArn": task_def_arn,
                        "TaskCount": task_count,
                    },
                }
            ]
            response = events.put_targets(Rule=rule, Targets=targets)
            print("\t" + f"created event: {cluster}/{service} - {rule}")
            target_id += 1
        else:
            print(f"{Fore.RED}Failed creating Cloudwatch event: no task_name specified")
    else:
        print(f"{Fore.RED}Invalid task_type: {task_type=} --  {cluster=} {task_name=}")

print("\n")
