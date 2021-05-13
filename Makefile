.PHONY: release

release:
	py.test tests/
	rm -rf build/ dist/
	python setup.py sdist bdist_wheel
	twine upload dist/*

install:
	pip install -e .
	pip install -r test-requirements.txt

test:
	py.test tests/
