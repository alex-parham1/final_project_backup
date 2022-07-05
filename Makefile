install:
	pip3 install --upgrade pip && \
	pip3 install -r docs/requirements.txt

tests:
	python3 -m pytest

format:
	python3 -m black $$(git ls-files '*.py')
	python3 -m pytest
	python3 -m pytest --cov=. src --cov-report xml:coverage.xml