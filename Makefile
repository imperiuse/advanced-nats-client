make.EXPORT_ALL_VARIABLES:

.PHONY: docker_clean_all
docker_clean_all:
	docker rm -f `docker ps -aq`

.PHONY: test_env_up
test_env_up:
	@echo "Start test environment"
	docker-compose -f .docker/docker-compose.test.yml up -d

.PHONY: test_env_down
test_env_down:
	@echo "Stop test environment"
	docker-compose -f .docker/docker-compose.test.yml down

.PHONY: tests
tests:
	@echo "Running go tests"
	go test -timeout 30s -race -short `go list ./... | grep -v mocks`

