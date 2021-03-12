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
databricks-sync init <filename>
databricks-sync init --help
```

### Description

**init** makes a YAML configuration file in the current working directory. This file is used for the export command.

### Arguments

`filename` - a user specified name for the generated YAML configuration file.

### Example

```bash
databricks-sync init my-config-file
```
