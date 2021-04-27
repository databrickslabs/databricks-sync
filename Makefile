default: install

install:
	@pip install -r dev-requirements.txt
	@pip install . --upgrade

docs-deps:
	@cd docs-site && npm install

docs:
	@rm -rf docs
	@cd docs-site && npm run build && cp -R public/ ../docs

docs-serve:
	@cd docs-site && npm run start

azure-create:
	@echo "✓ Creating Azure Src and Tgt workspaces..."
	@/bin/bash scripts/run.sh azcli --debug

azure-destroy:
	@echo "✓ Destroying Azure Src and Tgt workspaces..."
	@/bin/bash scripts/run.sh azcli --destroy


.PHONY: shared local-install docs