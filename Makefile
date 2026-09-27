UV ?= uv

.PHONY: dev runtime test lint format build build-services docker-build

dev runtime test lint format build build-services docker-build:
	$(UV) run --locked python scripts/dev.py $@
