
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

#import-build: @ build data import docker image
.PHONY: import-build
import-build:
	@echo "Build import docker image.."
	@docker build -t ${IMAGE_NAME_IMPORT}:${VERSION} -f ./${DOCKERFILE_IMPORT} .
	@echo "Finished."

#import-run: @ run data import in docker container
.PHONY: import-run
import-run: import-build
	@echo "Run import in docker container.."
	@docker run ${IMAGE_NAME_IMPORT}:${VERSION}
	@echo "Finished."

#forecast-build: @ build forecast docker image
.PHONY: forecast-build
forecast-build:
	@echo "Build forecast docker image.."
	@docker build -t ${IMAGE_NAME_FORECAST}:${VERSION} -f ./${DOCKERFILE_FORECAST} .
	@echo "Finished."

#forecast-run: @ run forecast in docker container
.PHONY: forecast-run
forecast-run: forecast-build
	@echo "Run forecast in docker container.."
	@docker run ${IMAGE_NAME_FORECAST}:${VERSION}
	@echo "Finished."