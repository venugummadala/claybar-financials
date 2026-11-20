.PHONY: build test package up down logs demo clean

build:            ## Compile and run all tests
	mvn -B verify

test: build

package:          ## Build executable jars without tests
	mvn -B -DskipTests package

up: package       ## Build images and start the full stack
	docker compose up -d --build
	@echo "Waiting for services to become healthy..."
	@docker compose up -d --wait

down:             ## Stop the stack (keeps volumes)
	docker compose down

clean:            ## Stop the stack and delete data
	docker compose down -v
	mvn -B -q clean

logs:
	docker compose logs -f orchestrator-service application-service kyc-service credit-service notification-service

demo:             ## Submit one application per decision path
	./scripts/demo.sh
