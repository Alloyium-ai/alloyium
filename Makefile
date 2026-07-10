ifneq (,$(wildcard .env))
include .env
export
endif

.DEFAULT_GOAL := help

NATS_CONTAINER ?= alloyium-demo-nats
REDIS_CONTAINER ?= alloyium-demo-redis
NATS_PORT ?= 4222
REDIS_PORT ?= 6379
NATS_URL ?= nats://127.0.0.1:$(NATS_PORT)
REDIS_URL ?= redis://127.0.0.1:$(REDIS_PORT)
ALLOYIUM_DEMO_STATE_DIR ?= .alloyium/demo
ALLOYIUM_DEMO_LOG_LEVEL ?= error

.PHONY: help init deps check-bun check-docker demo demo-up fleet-up fleet-status fleet-down demo-bus-up demo-bus-down demo-clean test

help:
	@printf '%s\n' \
	  'Alloyium local targets:' \
	  '  make init          Create .env from .env.example and runtime dirs.' \
	  '  make deps          Install Bun dependencies.' \
	  '  make demo          Start local bus and run a one-shot PM/team/fusion smoke test.' \
	  '  make fleet-up      Start local bus and keep the demo PM/team/fusion fleet running.' \
	  '  make fleet-status  Show live demo peers from Redis presence.' \
	  '  make fleet-down    Remove demo Redis/NATS containers.' \
	  '  make demo-clean    Remove generated demo identities, inboxes, and state.'

init:
	@if [ ! -f .env ]; then cp .env.example .env; echo 'created .env from .env.example'; else echo '.env already exists'; fi
	@mkdir -p "$(ALLOYIUM_DEMO_STATE_DIR)"

deps: check-bun
	@bun install

check-bun:
	@command -v bun >/dev/null 2>&1 || { echo 'bun is required: https://bun.sh/docs/installation'; exit 1; }

check-docker:
	@command -v docker >/dev/null 2>&1 || { echo 'docker is required for the local demo bus'; exit 1; }

demo-bus-up: check-bun
	@if LOG_LEVEL="$(ALLOYIUM_DEMO_LOG_LEVEL)" NATS_URL="$(NATS_URL)" REDIS_URL="$(REDIS_URL)" bun run scripts/demo-fleet.ts --check-bus >/dev/null 2>&1; then \
	  echo "using existing local bus at $(NATS_URL) and $(REDIS_URL)"; \
	else \
	  command -v docker >/dev/null 2>&1 || { echo 'docker is required to start the demo bus; start Redis/NATS yourself or install Docker'; exit 1; }; \
	  if docker ps -a --format '{{.Names}}' | grep -qx "$(REDIS_CONTAINER)"; then \
	    if ! docker start "$(REDIS_CONTAINER)" >/dev/null 2>&1; then \
	      docker rm "$(REDIS_CONTAINER)" >/dev/null 2>&1 || true; \
	      echo "using existing Redis at $(REDIS_URL)"; \
	    fi; \
	  else \
	    if ! docker run -d --name "$(REDIS_CONTAINER)" -p "$(REDIS_PORT):6379" redis:7-alpine >/dev/null 2>&1; then \
	      docker rm "$(REDIS_CONTAINER)" >/dev/null 2>&1 || true; \
	      echo "using existing Redis at $(REDIS_URL)"; \
	    fi; \
	  fi; \
	  if docker ps -a --format '{{.Names}}' | grep -qx "$(NATS_CONTAINER)"; then \
	    if ! docker start "$(NATS_CONTAINER)" >/dev/null 2>&1; then \
	      docker rm "$(NATS_CONTAINER)" >/dev/null 2>&1 || true; \
	      echo "using existing NATS at $(NATS_URL)"; \
	    fi; \
	  else \
	    if ! docker run -d --name "$(NATS_CONTAINER)" -p "$(NATS_PORT):4222" nats:2.10-alpine -js >/dev/null 2>&1; then \
	      docker rm "$(NATS_CONTAINER)" >/dev/null 2>&1 || true; \
	      echo "using existing NATS at $(NATS_URL)"; \
	    fi; \
	  fi; \
	  LOG_LEVEL="$(ALLOYIUM_DEMO_LOG_LEVEL)" NATS_URL="$(NATS_URL)" REDIS_URL="$(REDIS_URL)" bun run scripts/demo-fleet.ts --check-bus >/dev/null; \
	fi
	@printf 'Redis: %s\nNATS:  %s\n' "$(REDIS_URL)" "$(NATS_URL)"

demo: init demo-bus-up check-bun
	@LOG_LEVEL="$(ALLOYIUM_DEMO_LOG_LEVEL)" NATS_URL="$(NATS_URL)" REDIS_URL="$(REDIS_URL)" bun run scripts/demo-fleet.ts --once

demo-up: demo

fleet-up: init demo-bus-up check-bun
	@LOG_LEVEL="$(ALLOYIUM_DEMO_LOG_LEVEL)" NATS_URL="$(NATS_URL)" REDIS_URL="$(REDIS_URL)" bun run scripts/demo-fleet.ts --serve

fleet-status: init check-bun
	@LOG_LEVEL="$(ALLOYIUM_DEMO_LOG_LEVEL)" NATS_URL="$(NATS_URL)" REDIS_URL="$(REDIS_URL)" bun run scripts/demo-fleet.ts --status

fleet-down:
	@docker rm -f "$(NATS_CONTAINER)" "$(REDIS_CONTAINER)" >/dev/null 2>&1 || true
	@echo 'demo bus containers removed'

demo-bus-down: fleet-down

demo-clean:
	@rm -rf "$(ALLOYIUM_DEMO_STATE_DIR)"
	@echo 'demo state removed'

test:
	@bun test
