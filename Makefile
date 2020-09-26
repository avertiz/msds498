setup:
	python3 -m venv ~/.msds498

install:
	pip install --upgrade pip &&\
		pip install -r requirements.txt

lint:
	pylint --disable=R,C test.py
	
test:
	python -m pytest -vv --cov=test test.py