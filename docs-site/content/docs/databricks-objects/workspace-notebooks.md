---
title: "Notebooks"
description: "This guide tells you how databricks sync moves notebooks across workspaces."
lead: ""
date: 2020-10-13T15:21:01+02:00
lastmod: 2020-10-13T15:21:01+02:00
draft: false
images: []
menu:
  docs:
    parent: "objects"
weight: 310
toc: true
---

## Impacted Databricks Resources

The impacted resources for this workspace object are:

1. <a href="https://docs.databricks.com/notebooks/index.html" target="_blank"> Databricks Workspace Notebooks </a>
2. <a href="https://docs.databricks.com/security/access-control/workspace-acl.html#notebook-permissions" target="_blank"> Databricks Notebook Permissions </a>
3. <a href="https://docs.databricks.com/security/access-control/workspace-acl.html#folder-permissions" target="_blank"> Databricks Folder Permissions </a>

## References to appropriate REST Apis

1. <a href="https://docs.databricks.com/dev-tools/api/latest/workspace.html" target="_blank"> Workspace Api </a>: Used
downloading and uploading notebooks via terraform provider
2. <a href="https://docs.databricks.com/dev-tools/api/latest/permissions.html" target="_blank"> Permissions Api </a>: Used
exporting permissions on notebooks and folders and then applied to target workspace via terraform provider

## Known Limitations

TODO: Edit the language
{{< bootstrap-alert icon="💡" text=`<strong>Currently there is only one limitation based on the behavior of the
<a href="https://github.com/databrickslabs/terraform-provider-databricks" target="_blank">databricks terraform provider</a>.
The source workspace notebook changes are tracked by the checksum. Any changes made in the target workspace
are not tracked.</strong>` >}}


## Permissions Support

All permissions are dependant users and groups being created successfuly if you are exporting [`identity`]({{< relref "workspace-identities.md" >}}).

Currently the following permissions are exported:
1. Permissions directly attached to the notebooks
2. Permissions associated with any of the folders that are being exported. These folder permissions
   have a dependency on at least one notebook in that folder path being created in the workspace so the folder
   can also exist. This dependancy can be identified in the file itself

## Cloud Specific Behavior

N/A. Behavior for notebooks in all clouds is the same.

## Export Details

This section will describe the details of the export process as well as the layout.

### Example Config

Here is an example config for exporting `notebooks`:

```yaml
name: sample-yaml
objects:
  notebook:
    # Notebook path can be a string, a list or a YAML items collection
    # (multiple subgroups starting with - )
    notebook_path: "/"
    # Use Custom map var to setup a new location.
    # Certain patterns can be excluded from being
    # exported via exclude_path field.
    # Make sure to use the glob syntax to specify all paths.
    exclude_path:
      - "/Users/**" # Ignore all paths within the users folder
      - "/tmp/**" # Ignore all files in the tmp directory
```

### Config Options

There are three options that you can provide for the notebook object. They are:

1. [`notebook_path`](#notebook-path-required) **(required)**
2. [`exclude_path`](#exclude-path-optional) **(optional)**
3. [`custom_map_vars`](#custom-map-vars-optional) **(optional)**

#### Notebook Path (required)

`notebook_path`: The path to recursively fetch all notebooks and permissions for given notebooks and folders in the provided path.
This field can be either a list or a string.

The following example shows `notebook_path` used as a string:

```yaml
name: sample-yaml
objects:
  notebook:
    # Notebook path can be a string, a list or a YAML items collection
    # (multiple subgroups starting with - )
    notebook_path: "/" # all paths inside the workspace
```

The following example shows `notebook_path` used as a list:

```yaml
name: sample-yaml
objects:
  notebook:
    # Notebook path can be a string, a list or a YAML items collection
    # (multiple subgroups starting with - )
    notebook_path:
    - "/Shared" # all paths inside the /Shared location will be exported
    - "/Dev" # all paths inside the /Dev location will be exported
```

#### Exclude Path (optional)

`exclude_path`: The path that can be used with glob patterns (`*` or `**`) to omit the export of certain notebooks or
subdirectories. This field can be either a list or a string.

The following example shows `exclude_path` used as a string:

```yaml
name: sample-yaml
objects:
  notebook:
    # Notebook path can be a string, a list or a YAML items collection
    # (multiple subgroups starting with - )
    notebook_path: "/" # all paths inside the workspace
    exclude_path: "/Users/**" # Ignore all paths within the users folder
```

The following example shows `exclude_path` used as a list:

```yaml
name: sample-yaml
objects:
  notebook:
    # Notebook path can be a string, a list or a YAML items collection
    # (multiple subgroups starting with - )
    notebook_path: "/"
    exclude_path:
      - "/Users/**" # Ignore all paths within the users folder
      - "/tmp/**" # Ignore all files in the tmp directory
```

#### Custom Map Vars (optional)

Please take a look at this guide to leverage custom map vars. TODO!!!

### Handled Dependencies

The notebook resource is not dependent on other resources. The order in which resources will be created during the import is:

1. Notebook object
2. Notebook permission
3. Parent folder permission (This will wait for atleast one notebook to be populated in that folder)

All permissions will wait for all **[`identity`]({{< relref "workspace-identities.md" >}})** to be created if they are exported.
This means that it will wait for all users, groups, service_principals, group members,
instance profiles and instance profile relationships to be created first.

### Exported content + layout

Here is the folder layout you should see when you run the export. Under the `exports` directory you should see a notebook folder
which houses all the `notebook` exports. You should see up to 3 types of files and one folder in the `notebooks` directory.

1. You should see a `databricks_notebook_*` file that ends with `.tf.json` which contains your `notebook` information.
2. You may see a `databricks_notebook_*_permissions.tf.json` file which contains your `notebook` permission information.
**Not every notebook will have permissions if they are inherited from the parent folders.**
3. You may see a `databricks_folder_*_permissions.tf.json` file which contains parent folder permission information.
**Only explicit permissions are migrated and any inherited permissions from the folder will be exported separately for that specific folder..**

In the `notebook` directory you will also see a `data` folder which contains all the actual content for the notebooks.
All exported data will be `.py`, `.scala`, `.sql` or `.r` files.

```bash
.
├── databricks_spark_env.sh
├── exports
│    └── notebook
│        ├── data
│        │   ├── databricks_notebook_Shared_it_empty_python_nb.py
│        │   ├── databricks_notebook_Shared_it_example_notebook.py
│        │   ├── databricks_notebook_Shared_it_python_notebook.py
│        │   ├──databricks_notebook_Shared_it_scala_notebook.scala
│        │   └── databricks_notebook_Shared_provider_test_test123.py
│        ├── databricks_folder_*********_permissions.tf.json
│        ├── databricks_folder_*********_permissions.tf.json
│        ├── databricks_notebook_*********_permissions.tf.json
│        ├── databricks_notebook_Shared_it_empty_python_nb.tf.json
│        ├── databricks_notebook_Shared_it_example_notebook.tf.json
│        ├── databricks_notebook_Shared_it_python_notebook.tf.json
│        ├── databricks_notebook_Shared_it_scala_notebook.tf.json
│        └── databricks_notebook_Shared_provider_test_test123.tf.json
├── terraform.tfvars
└── variables_env.sh
```

## Import Details

Specific import details for importing `notebook` data into a target workspace.

### Environment Variables

There are no specific environment variables that need to be specified for imports. Refer to the imports environment variables
for custom mapping variables. TODO!!!

### How are increments determined

During the import process the increments are determined by the changes in the checksum of the `data` files. Any changes
done in the target workspace will **not** be taken into account.

## Additional Guidance

### Migrations

TODO!

### Disaster Recovery

TODO!
