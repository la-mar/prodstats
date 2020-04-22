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
	docker tag ${IMAGE_NAME} ${IMAGE_NAME}:${APP_VERSION}
	docker tag ${IMAGE_NAME} ${IMAGE_NAME}:latest
	# docker tag ${IMAGE_NAME} ${IMAGE_NAME}:${COMMIT_HASH}
	# docker tag ${IMAGE_NAME} ${IMAGE_NAME}:dev


build-with-chamber:
	@echo "Building docker image: ${IMAGE_NAME} (with chamber)"
	docker build  -f Dockerfile.chamber . -t ${IMAGE_NAME}
	docker tag ${IMAGE_NAME} ${IMAGE_NAME}:chamber-${APP_VERSION}
	docker tag ${IMAGE_NAME} ${IMAGE_NAME}:chamber-latest
	# docker tag ${IMAGE_NAME} ${IMAGE_NAME}:chamber-${COMMIT_HASH}
	# docker tag ${IMAGE_NAME} ${IMAGE_NAME}:chamber-dev

build-all: build-with-chamber build

push: login
	docker push ${IMAGE_NAME}:dev
	docker push ${IMAGE_NAME}:${COMMIT_HASH}
	docker push ${IMAGE_NAME}:latest

push-all: login push
	docker push ${IMAGE_NAME}:chamber-dev
	docker push ${IMAGE_NAME}:chamber-${COMMIT_HASH}
	docker push ${IMAGE_NAME}:chamber-latest

push-version:
	# docker push ${IMAGE_NAME}:latest
	@echo pushing: ${IMAGE_NAME}:${APP_VERSION}, ${IMAGE_NAME}:chamber-${APP_VERSION}
	docker push ${IMAGE_NAME}:${APP_VERSION}
	docker push ${IMAGE_NAME}:chamber-${APP_VERSION}

all: build-all push-all

ci-expand-config:
	# show expanded configuration
	circleci config process .circleci/config.yml

ci-process:
	circleci config process .circleci/config.yml > process.yml

ci-build-local:
	JOBNAME?=build-image
	circleci local execute -c process.yml --job build-image -e DOCKER_LOGIN=${DOCKER_LOGIN} -e DOCKER_PASSWORD=${DOCKER_PASSWORD}

all: build login push

deploy:
	poetry run python scripts/deploy.py

secret-key:
	python3 -c 'import secrets;print(secrets.token_urlsafe(256))'

post-from-file:
	http POST :8000/api/v1/users < tests/data/users.json --follow

create-db:
	psql -h localhost -d postgres -c "create database well;"
