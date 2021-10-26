_CHECK_PODMAN := $(shell command -v podman 2> /dev/null)
define compose-tool
	$(if $(_CHECK_PODMAN), podman-compose, docker-compose) -f container-compose.yml
endef

define container-tool
	$(if $(_CHECK_PODMAN), podman, docker)
endef

up:
	$(call compose-tool) up -d
restart:
	$(MAKE) halt && $(MAKE) up
halt:
	$(call compose-tool) down -t1
bash:
	$(call container-tool) exec -it fedora-msg bash -c "bash;"
logs:
	$(call container-tool) logs -f fedora-msg rabbitmq
