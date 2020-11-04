default: install

install:
	@pip install -r dev-requirements.txt
	@pip install . --upgrade

azure-create:
	@echo "✓ Running Terraform Acceptance Tests for Azure..."
	@/bin/bash scripts/run.sh azcli --debug

azure-destroy:
	@echo "✓ Destroying Azure Environment..."
	@/bin/bash scripts/run.sh azcli --destroy


.PHONY: shared local-install