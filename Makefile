DIR := ${CURDIR}
PIP=./venv/bin/pip
PYTHON=./venv/bin/python
COVERAGE=./venv/bin/coverage


all: clean deps build


build:


venv:
	python3 -m venv venv
	$(PIP) install --upgrade pip


deps: venv
	$(PIP) install -Ur requirements.txt


test:
	$(COVERAGE) run -m unittest
	$(COVERAGE) combine
	$(COVERAGE) html


clean:
	rm -rf htmlcov
	rm -rf .coverage


distclean: clean
	rm -rf venv
	git clean -fd
