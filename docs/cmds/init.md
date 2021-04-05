---
layout: "databricks"
page_title: "Provider: Databricks"
sidebar_current: "docs-databricks-command-ref-init"
description: |-
  Databricks sync databricks commands init.
---

## Init

### Synopsis

```bash
databricks-sync init [OPTIONS] FILENAME
databricks-sync init --help
```

### Description

**init** makes a YAML configuration file in the current working directory. This file is used for the export command.

### Options

* `--debug` - Debug mode. Shows full stack trace on error.
* `--help` or `-h` - Shows Usage and Options for `init` then exists.

### Arguments

* `filename` - A user specified name for the generated YAML configuration file.

### Example

```bash
databricks-sync init EXPORT_CONFIG_FILENAME
```
