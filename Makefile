install:
	python3 -m venv .venv
	make venv
	pip3 install --upgrade pip && \
	pip3 install -r docs/requirements.txt

testing:
	python3 -m pytest --cov=. --cov-report xml:coverage.xml

format:
	python3 -m black $$(git ls-files '*.py')

venv:
	source team_repo_3/.venv/bin/activate

run:
	ls
	