
.DEFAULT_GOAL := help

VERSION = "latest"

IMAGE_NAME = "kennzahlenupdate"
DOCKERFILE_IMPORT = "import/Dockerfile"

#help:				@ list available goals
.PHONY: help
help:
	@grep -E '[a-zA-Z\.\-]+:.*?@ .*$$' $(MAKEFILE_LIST)| sort | tr -d '#'  | awk 'BEGIN {FS = ":.*?@ "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

#import-build: @ build data import docker image
.PHONY: import-build
import-build:
	@echo "Build import docker image.."
	@docker build -t ${IMAGE_NAME}:${VERSION} -f ./${DOCKERFILE_IMPORT} .
	@echo "Finished."

#import-run: @ run data import in docker container
.PHONY: import-run
import-run: import-build
	@echo "Run import in docker container.."
	@docker run ${IMAGE_NAME}:${VERSION}
	@echo "Finished."