SERVICE_NAME:=prodstats
ENV:=prod
COMMIT_HASH ?= $$(git log -1 --pretty=%h)
DATE := $$(date +"%Y-%m-%d")
CTX:=.
AWS_ACCOUNT_ID:=$$(aws-vault exec prod -- aws sts get-caller-identity | jq .Account -r)
DOCKERFILE:=Dockerfile
APP_NAME:=$$(grep -e 'name\s=\s\(.*\)' pyproject.toml| cut -d"\"" -f2)
APP_VERSION=$$(grep -o '\([0-9]\+.[0-9]\+.[0-9]\+\)' pyproject.toml | head -n1)
# IMAGE_NAME:=$(APP_NAME)

run-tests:
	pytest --cov=prodstats tests/ --cov-report xml:./coverage/python/coverage.xml

cov:
	export CI=false && poetry run pytest -x --cov src/prodstats tests/ --cov-report html:./coverage/coverage.html --log-cli-level 30 --log-level 20 -vv

pscov:
	export CI=false && poetry run pytest -x --cov src/prodstats tests/calc/test_prod_calc.py --cov-report html:./coverage/coverage.html --log-cli-level 30 --log-level 20 -v

cicov:
	export CI=true && poetry run pytest -x --cov src/prodstats tests/ --cov-report html:./coverage/coverage.html --log-cli-level 20 --log-level 20 -vv

ncov:
	# generate cov report to stdout
	poetry run pytest --cov src/prodstats tests/

view-cov:
	open -a "Google Chrome" ./coverage/coverage.html/index.html

release:
	poetry run python scripts/release.py

login:
	docker login -u ${DOCKER_USERNAME} -p ${DOCKER_PASSWORD}

build:
	@echo "Building docker image: ${IMAGE_NAME}"
	docker build  -f Dockerfile . -t ${IMAGE_NAME}
	docker tag ${IMAGE_NAME} ${IMAGE_NAME}:latest

push: login
	# docker push ${IMAGE_NAME}:dev
	# docker push ${IMAGE_NAME}:${COMMIT_HASH}
	docker push ${IMAGE_NAME}:latest

push-version:
	docker push ${IMAGE_NAME}:${APP_VERSION}

all: build push

ci-expand-config:
	# show expanded configuration
	circleci config process .circleci/config.yml

ci-process:
	circleci config process .circleci/config.yml > process.yml

ci-build-local:
	JOBNAME?=build-image
	circleci local execute -c process.yml --job build-image -e DOCKER_LOGIN=${DOCKER_LOGIN} -e DOCKER_PASSWORD=${DOCKER_PASSWORD}

deploy:
	export AWS_ACCOUNT_ID=${AWS_ACCOUNT_ID} && poetry run python scripts/deploy.py

redeploy-cron:
	@echo ""
	aws ecs update-service --cluster ecs-collector-cluster --service prodstats-cron --force-new-deployment --profile ${ENV} | jq .service.serviceName,.service.taskDefinition,.service.clusterArn

redeploy-worker:
	@echo ""
	aws ecs update-service --cluster ecs-collector-cluster --service prodstats-worker --force-new-deployment --profile ${ENV} | jq .service.serviceName,.service.taskDefinition,.service.clusterArn

redeploy-web:
	@echo ""
	aws ecs update-service --cluster ecs-web-cluster --service prodstats-web --force-new-deployment --profile ${ENV} | jq .service.serviceName,.service.taskDefinition,.service.clusterArn

redeploy: redeploy-worker redeploy-cron redeploy-web

deploy-migrations:
	aws ecs run-task --cluster ecs-collector-cluster --task-definition prodstats-db-migrations --profile ${ENV}


secret-key:
	python3 -c 'import secrets;print(secrets.token_urlsafe(256))'

post-from-file:
	http POST :8000/api/v1/users < tests/data/users.json --follow

create-db:
	@echo "Creating database: ${DATABASE_NAME}"
	psql -h localhost -d postgres -c "create database ${DATABASE_NAME};"
