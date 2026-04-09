clean:
	find . -name '*.pyc' | xargs rm
	find . -name '__pycache__' | xargs rm -rf
	find . -name '.pytest_cache' | xargs rm -rf

build:
	docker build -t test:test .

test-ci: build
	docker run -i test:test airflow version > output.txt
	@if grep -q -i ERROR output.txt; then \
        echo "Error found when running checking the version of Airflow"; \
        echo "This is likely due to a python package conflict"; \
        echo "Please check your requirements.txt and Dockerfile and validate your deployment by running astro dev start locally"; \
		exit 1; \
	fi
