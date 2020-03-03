SERVICE_NAME:=prodstats
ENV:=prod
COMMIT_HASH    ?= $$(git log -1 --pretty=%h)
DATE := $$(date +"%Y-%m-%d")
CTX:=.
AWS_ACCOUNT_ID:=$$(aws-vault exec prod -- aws sts get-caller-identity | jq .Account -r)
IMAGE_NAME:=prodstats
DOCKERFILE:=Dockerfile
APP_VERSION=$$(grep -o '\([0-9]\+.[0-9]\+.[0-9]\+\)' pyproject.toml | head -n1)

run-tests:
	pytest --cov=prodstats tests/ --cov-report xml:./coverage/python/coverage.xml

smoke-test:
	docker run --entrypoint prodstats driftwood/prodstats:${COMMIT_HASH} test smoke-test

cov:
	export CI=false && poetry run pytest -x --cov src/prodstats tests/ --cov-report html:./coverage/coverage.html --log-cli-level 30 --log-level 20 -vv

cicov:
	export CI=true && poetry run pytest -x --cov src/prodstats tests/ --cov-report html:./coverage/coverage.html --log-cli-level 10 --log-level 10 -vv

ncov:
	# generate cov report to stdout
	poetry run pytest --cov src/prodstats tests/

view-cov:
	open -a "Google Chrome" ./coverage/coverage.html/index.html

release:
	poetry run python scripts/release.py

redis-up:
	# start a local redis container
	redis-server ./redis/redis.conf

kubectl-proxy:
	# open a proxy to the configured kubernetes cluster
	kubectl proxy --port=8080

login:
	docker login -u ${DOCKER_USERNAME} -p ${DOCKER_PASSWORD}

build: login
	@echo "Building docker image: ${IMAGE_NAME}"
	docker build  -f ${DOCKERFILE} ${CTX} -t ${IMAGE_NAME}
	docker tag ${IMAGE_NAME} ${IMAGE_NAME}:${COMMIT_HASH}

push:
	docker push ${IMAGE_NAME}:dev
	docker push ${IMAGE_NAME}:${COMMIT_HASH}

push-version:
	# docker push ${IMAGE_NAME}:latest
	docker push ${IMAGE_NAME}:${APP_VERSION}

circleci-expand-config:
	# show expanded configuration
	circleci config process .circleci/config.yml

circleci-process:
	circleci config process .circleci/config.yml > process.yml

circleci-build-local:
	JOBNAME?=build-image
	circleci local execute -c process.yml --job build-image -e DOCKER_LOGIN=${DOCKER_LOGIN} -e DOCKER_PASSWORD=${DOCKER_PASSWORD}

all: build login push

deploy:
	poetry run python scripts/deploy.py

secret-key:
	python3 -c 'import secrets;print(secrets.token_urlsafe(256))'

post-from-file:
	http POST :8000/api/v1/users < tests/data/users.json --follow
