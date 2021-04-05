---
layout: "databricks"
page_title: "Provider: Databricks"
sidebar_current: "docs-databricks-usage"
description: |-
  Databricks sync databricks usage.
---

## Usage

Databricks-sync is used by providing a command. Each command follows the same syntax but may have distinct arguments.

A command is executed by writing the name of the command followed by any required and optional arguments. Options for the base call to databricks-sync program may be passed prior to calling the command.

### Command structure

```bash
databricks-sync [OPTIONS] COMMAND [ARGS]...
databricks-sync --help
```

### Program Options

* `--version` - Provides the version of databricks-sync.
* `--verbosity` or `-v` - Either `critical`, `error`, `warning`, `info` or `debug`.
* `--help` - Shows Usage, Options, and Commands for databricks-sync then exit.

### Commands
**[`init`](https://github.com/databrickslabs/databricks-sync/tree/master/docs/cmds/init.md)** - Create the export configuration file for running the export command.  
**[`export`](https://github.com/databrickslabs/databricks-sync/tree/master/docs/cmds/export.md)** - Export objects from a source Databricks workspace.  
**[`import`](https://github.com/databrickslabs/databricks-sync/tree/master/docs/cmds/import.md)** - Import objects into a target Databricks workspace.  
**[`triage`](https://github.com/databrickslabs/databricks-sync/tree/master/docs/cmds/triage.md)** - Run custom commands on the resources. 
  
  
