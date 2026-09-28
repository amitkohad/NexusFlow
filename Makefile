UV ?= uv

.PHONY: dev runtime test lint format build build-services build-packages docker-build

dev runtime test lint format build build-services build-packages docker-build:
	$(UV) run --locked python scripts/dev.py $@
