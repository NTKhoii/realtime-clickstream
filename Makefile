# activate generate clickstream events
generate:
	uv run data_generator/main.py
up:
	docker compose up -d
down:
	docker compose down 