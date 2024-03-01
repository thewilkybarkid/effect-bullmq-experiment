.PHONY: start start-services

node_modules: package.json package-lock.json
	npm install
	touch node_modules

start: node_modules start-services
	REDIS_URL=redis://$(shell docker compose port redis 6379) npx tsx watch --clear-screen=false src

start-services:
	docker compose up --detach
