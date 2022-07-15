.ONESHELL:
install:
	python3 -m venv .venv
	source .venv/bin/activate	
	pip3 install --upgrade pip && \
	pip3 install -r requirements.txt

run:
	source .venv/bin/activate; \
	python3 ./src/scripts/main.py


testing:
	source .venv/bin/activate; \
	python3 -m pytest --cov=. --cov-report xml:coverage.xml

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
	