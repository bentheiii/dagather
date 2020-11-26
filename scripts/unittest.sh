# run the unittests with branch coverage
python -m poetry run python -m pytest --cov=./dagather --cov-report=xml --cov-report=term-missing tests/