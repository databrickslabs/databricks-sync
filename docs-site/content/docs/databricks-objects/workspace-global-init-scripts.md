---
title: "Global Init Scripts"
description: "This guide tells you how databricks sync moves Global Init Scripts across workspaces."
lead: ""
date: 2020-10-13T15:21:01+02:00
lastmod: 2020-10-13T15:21:01+02:00
draft: false
images: []
menu:
  docs:
    parent: "objects"
weight: 320
toc: true
---

## Impacted Databricks Resources

The impacted resources for this workspace object are:

1. <a href="https://docs.databricks.com/clusters/init-scripts.html#add-a-global-init-script-using-the-ui" target="_blank"> Databricks Global Init Scripts </a>

## References to appropriate REST Apis

1. <a href="https://docs.databricks.com/dev-tools/api/latest/global-init-scripts.html" target="_blank"> Global Init Scripts Api </a>: Used
downloading and uploading global init scripts via terraform provider

## Known Limitations

**N/A**

## Permissions Support

This object does not have permissions. You need to be an admin to modify this as it is configured at a workspace level.

## Cloud Specific Behavior

**N/A**. Behavior for global init scripts in all clouds is the same.

## Export Details

This section will describe the details of the export process as well as the layout.

### Example Config

Here is an example config for exporting `global init scripts`:

```yaml
name: test
objects:
  global_init_script:
    # pattern will be implemented in the future - make sure you have "*" in here
    patterns:
      - "*"
```

### Config Options

There is only one options that you can provide for the `global init scripts` object. It is:

1. [`patterns`](#global-init-scripts-patterns-required) **(required)**

#### Global Init Scripts Patterns (required)

`patterns`: pattern will be implemented in the future - make sure you have "*" in here. It is a list object.

The only way to use patterns is to provide one list item with `"*"`:

```yaml
name: test
objects:
  global_init_script:
    # pattern will be implemented in the future - make sure you have "*" in here
    patterns:
      - "*"
```

### Handled Dependencies

There are no explicit dependencies handled for global init scripts.

### Exported content + layout

Here is the folder layout you should see when you run the export. Under the `exports` directory you should see a notebook folder
which houses all the `global init script` exports.
You should see one file and one folder in the `global_init_scripts` directory.

1. You should see a `databricks_global_init_script.tf.json` file that ends with `.tf.json` which contains your `notebook` information.
The file contains an array of all the global init scripts that need to be created. **Even though it is one file it can create
0 to many global init scripts in the target workspace.**

In the `global_init_scripts` directory you will also see a `data` folder which contains all the actual content for the global init scripts.

```bash
.
├── databricks_spark_env.sh
├── exports
│   └── global_init_scripts
│       ├── data
│       │   └── databricks_global_init_script_haha_4B11DDF81A4D012E
│       └── databricks_global_init_scripts.tf.json
├── terraform.tfvars
└── variables_env.sh
```

## Import Details

Specific import details for importing `global init scripts` data into a target workspace.

### Environment Variables

There are no specific environment variables that need to be specified for imports.

### How are increments determined

During the import process the increments are determined by the changes in the checksum of the `data` files. Any changes
done in the target workspace will **not** be taken into account.

## Additional Guidance

### Migrations

TODO!

### Disaster Recovery

TODO!
