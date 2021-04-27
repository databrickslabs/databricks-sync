---
title: "Init"
description: "Init creates the basic structure of the export yaml file."
lead: "Init creates the basic structure of the export yaml file."
date: 2020-10-13T15:21:01+02:00
lastmod: 2020-10-13T15:21:01+02:00
draft: false
images: []
menu:
  docs:
    parent: "commands"
weight: 210
toc: true
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
