UV ?= uv

.PHONY: dev test lint format build docker-build

dev test lint format build docker-build:
	$(UV) run --locked python scripts/dev.py $@
