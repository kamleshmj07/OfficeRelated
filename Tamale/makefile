#
# makefile for TestProject
#
SHELL=/bin/bash

BUILD=prod

BASE_DIR=//seg-ny-fsdev/f1
PROJECT_DIR=Tamale

ifeq ($(BUILD),qa)
	DEPLOY_DIR=$(BASE_DIR)/qa/$(PROJECT_DIR)
else ifeq ($(BUILD),prod)
	DEPLOY_DIR=$(BASE_DIR)/prod/$(PROJECT_DIR)
endif


BIN=bin
LIB=lib
SQL=sql
CONF=conf

DIRS_LIB=$(shell find lib -type d)

MAKEFILE_FILES=makefile
DIRS=bin lib conf sql work archive log
BIN_FILES=$(shell find bin -type f -name '*.pl' -or -name '*.ps1' -or -name '*PROD' -or -name '*QA')
LIB_FILES=$(shell find lib -type f -name '*.pl')
SQL_FILES=$(shell find sql -type f)
CONF_FILES=$(shell find conf -type f)
ALL_FILES=$(shell find . -type f -name '*.pl' -or -name '*.ps1' -or -name '*PROD' -or -name '*QA')

.PHONY: rel

info_output:
	@echo BUILD=$(BUILD)
	@echo DEPLOY_DIR=$(DEPLOY_DIR)

mkf:
	@for f in $(MAKEFILE_FILES) ; do \
		cmd="cp $$f $(DEPLOY_DIR)/$$f"; \
		cmp -s $$f $(DEPLOY_DIR)/$$f || (eval $$cmd && echo $$cmd); \
	done

rel_dirs:
	@for d in $(DIRS_LIB) ; do \
		cmd="mkdir -p $(DEPLOY_DIR)/$$d"; \
		[ -d $(DEPLOY_DIR)/$$d ] || (eval $$cmd && echo $$cmd); \
	done
	@for d in $(DIRS) ; do \
		cmd="mkdir -p $(DEPLOY_DIR)/$$d"; \
		[ -d $(DEPLOY_DIR)/$$d ] || (eval $$cmd && echo $$cmd); \
	done
	
rel: info_output rel_dirs mkf 
	@for f in $(BIN_FILES) ; do \
		cmd="cp $$f $(DEPLOY_DIR)/$$f"; \
		cmp -s $$f $(DEPLOY_DIR)/$$f || (eval $$cmd && echo $$cmd); \
	done
	@for f in $(LIB_FILES) ; do \
		cmd="cp $$f $(DEPLOY_DIR)/$$f"; \
		cmp -s $$f $(DEPLOY_DIR)/$$f || (eval $$cmd && echo $$cmd); \
	done
	@for f in $(SQL_FILES) ; do \
		cmd="cp $$f $(DEPLOY_DIR)/$$f"; \
		cmp -s $$f $(DEPLOY_DIR)/$$f || (eval $$cmd && echo $$cmd); \
	done
	@for f in $(CONF_FILES) ; do \
		cmd="cp $$f $(DEPLOY_DIR)/$$f"; \
		cmp -s $$f $(DEPLOY_DIR)/$$f || (eval $$cmd && echo $$cmd); \
	done
	@for f in $(ALL_FILES) ; do \
	    chmod -R 777 "$(DEPLOY_DIR)/$$f"; \
	done
cmp_files: info_output
	echo ====diffs details====
	@for f in $(MAKEFILE_FILES) $(BIN_FILES) $(LIB_FILES) $(CONF_FILES) $(SQL_FILES); do \
		cmd="diff --ignore-case $$f $(DEPLOY_DIR)/$$f"; \
		echo ===$$cmd=== && eval $$cmd; \
	done
