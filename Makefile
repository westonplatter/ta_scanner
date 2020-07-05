CONDA_ENV ?= ta_scanner

test:
	@pytest -s .

release:
	@python setup.py sdist
	@twine upload dist/*

example:
	@python examples/moving_average_crossover.py 


env.create:
	@conda create -y -n ${CONDA_ENV} python=3.7

env.update:
	@conda env update -n ${CONDA_ENV} -f environment.yml

