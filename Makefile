.PHONY: dev

dev:
	@uv run fastapi dev app/main.py --host 127.0.0.1 --port 8000 & \
	api_pid=$$!; \
	trap 'kill $$api_pid 2>/dev/null' EXIT INT TERM; \
	cloudflared tunnel --url http://127.0.0.1:8000 run cherry-pancake
