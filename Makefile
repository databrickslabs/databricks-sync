default: local-install

BASE_PATH := databricks_terraformer

shared:
	@echo "==> Building shared libraries ..."
	@cd cgo && go build -o ../$(BASE_PATH)/hcl/json2hcl.so -buildmode=c-shared main.go

shared-linux:
	@echo "==> Building shared libraries ..."
	@GOOS=linux GOARCH=amd64 cd cgo && go build -o ../$(BASE_PATH)/hcl/json2hcl.so -buildmode=c-shared main.go

local-install: shared-linux
	@echo "==> Local install of library ..."
	@pip install . --upgrade

dev-env:
	@echo "==> Setting up local env ..."
	@pip install -r dev-requirements.txt

.PHONY: shared local-install