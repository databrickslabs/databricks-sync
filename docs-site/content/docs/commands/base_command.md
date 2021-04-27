---
title: "Base Command"
description: "Basic outline of the different commands."
lead: "Basic outline of the different commands."
date: 2020-10-13T15:21:01+02:00
lastmod: 2020-10-13T15:21:01+02:00
draft: false
images: []
menu:
  docs:
    parent: "commands"
weight: 200
toc: true
---

## Base Command

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

### Databricks Sync Commands
* **[`init`]({{< relref "init.md" >}})** - Create the export configuration file for running the export command.
* **[`export`]({{< relref "export.md" >}})** - Export objects from a source Databricks workspace.
* **[`import`]({{< relref "import.md" >}})** - Import objects into a target Databricks workspace.
