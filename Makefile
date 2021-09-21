
.DEFAULT_GOAL := help

VERSION = "latest"

IMAGE_NAME_IMPORT = "kennzahlenupdate-import"
DOCKERFILE_IMPORT = "import/Dockerfile"

IMAGE_NAME_FORECAST = "kennzahlenupdate-forecast"
DOCKERFILE_FORECAST = "forecast/Dockerfile"

#help:				@ list available goals
.PHONY: help
help:
	@grep -E '[a-zA-Z\.\-]+:.*?@ .*$$' $(MAKEFILE_LIST)| sort | tr -d '#'  | awk 'BEGIN {FS = ":.*?@ "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

#docker-import: @ run import application in docker container according to config in .env file
.PHONY: docker-import
docker-import:
	@echo "Run import application in docker container.."
	@docker-compose down
	@docker-compose up --build import
	@echo "Finished."

#docker-forecast: @ run forecast application in docker container according to config in .env file
.PHONY: docker-forecast
docker-forecast:
	@echo "Run forecast application in docker container.."
	@docker-compose down
	@docker-compose up --build forecast
	@echo "Finished."