README:
	cp README.rst README

build:
	python3 setup.py build

dev:
	python3 setup.py develop

upload: README
	python3 setup.py sdist upload

lint:
	python3 -m flake8 --show-source .

test:
	python3 bin/test
	python3 -m unittest tests

clean:
	rm -rf build dist README MANIFEST tasky.egg-info
	find . -name '*.pyc' -delete
