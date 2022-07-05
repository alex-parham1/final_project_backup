install:
	pip3 install --upgrade pip && \
	pip3 install -r docs/requirements.txt

tests:
	python3 -m pytest -vv --cov=mylib tests/
	python3 -m pytest --cov=. src --cov-report xml:coverage.xml

format:
	python3 -m black $$(git ls-files '*.py')