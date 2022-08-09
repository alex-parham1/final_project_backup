.ONESHELL:
install:
	python3 -m venv .venv
	source .venv/bin/activate; \
	pip3 install --upgrade pip && \
	pip3 install -r requirements.txt

update:
	source .venv/bin/activate; \
	pip3 install --upgrade pip && \
	pip3 install -r requirements.txt

run:
	source .venv/bin/activate; \
	python3 ./src/scripts/main.py

secrets:
	gh secret set AWS_ACCESS_KEY_ID --body "$$(aws configure get aws_access_key_id --profile bootcamp-sandbox)";
	gh secret set AWS_SECRET_ACCESS_KEY --body "$$(aws configure get aws_secret_access_key --profile bootcamp-sandbox)";
	gh secret set AWS_SESSION_TOKEN --body "$$(aws configure get aws_session_token --profile bootcamp-sandbox)"


testing:
	source .venv/bin/activate; \
	python3 -m pytest --cov=. --cov-report xml:coverage.xml --cov-report html:coverage.html

format:
	python3 -m black $$(git ls-files '*.py')

relog:
	aws-azure-login --profile bootcamp-sandbox --mode=gui

thirstee:
	aws ssm start-session \
    --target i-0c0cc9d20a9af37e9 \
    --profile bootcamp-sandbox \
    --region eu-west-1 \
    --document-name AWS-StartPortForwardingSessionToRemoteHost \
    --parameters '{"host":["thirstee.cm76nv1fmnjs.eu-west-1.rds.amazonaws.com"],"portNumber":["3306"], "localPortNumber":["3307"]}'

grafana:
	aws ssm start-session \
    --target i-018ff9d84f4924454 \
    --profile bootcamp-sandbox \
    --region eu-west-1

graf_thirstee:
	aws ssm start-session \
    --target i-03a84a40778d79165 \
    --profile bootcamp-sandbox \
    --region eu-west-1 \
    --document-name AWS-StartPortForwardingSessionToRemoteHost \
    --parameters '{"host":["thirstee.cm76nv1fmnjs.eu-west-1.rds.amazonaws.com"],"portNumber":["3306"], "localPortNumber":["3307"]}'

tf-init:
	docker-compose -f deploy/docker_compose.yml run --rm terraform init

tf-fmt:
	docker-compose -f deploy/docker_compose.yml run --rm terraform fmt

tf-validate:
	docker-compose -f deploy/docker_compose.yml run --rm terraform validate

tf-plan:
	docker-compose -f deploy/docker_compose.yml run --rm terraform plan

tf-apply:
	docker-compose -f deploy/docker_compose.yml run --rm terraform apply

tf-destroy:
	docker-compose -f deploy/docker_compose.yml run --rm terraform destroy
