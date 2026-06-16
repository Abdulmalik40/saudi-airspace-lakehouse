.PHONY: help build up down restart logs ps clean nuke

help:
	@echo "Available targets:"
	@echo "  build    - Build the custom Airflow image"
	@echo "  up       - Start the stack in the background"
	@echo "  down     - Stop the stack, keep volumes"
	@echo "  restart  - Restart all services"
	@echo "  logs     - Tail logs from all services"
	@echo "  ps       - Show running containers"
	@echo "  clean    - Stop the stack and remove volumes (DESTROYS DB)"
	@echo "  nuke     - clean + remove images (full reset)"

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

restart:
	docker compose restart

logs:
	docker compose logs -f

ps:
	docker compose ps

clean:
	docker compose down --volumes --remove-orphans

nuke:
	docker compose down --volumes --rmi all --remove-orphans