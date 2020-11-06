default: install

install:
	@pip install -r dev-requirements.txt
	@pip install . --upgrade

azure-create:
	@echo "✓ Creating Azure Src and Tgt workspaces..."
	@/bin/bash scripts/run.sh azcli --debug

azure-destroy:
	@echo "✓ Destroying Azure Src and Tgt workspaces..."
	@/bin/bash scripts/run.sh azcli --destroy


.PHONY: shared local-install