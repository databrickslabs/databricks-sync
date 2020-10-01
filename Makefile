default: install

install:
	@pip install -r dev-requirements.txt
	@pip install . --upgrade

.PHONY: shared local-install