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
2. Databricks Notebook Permissions
3. Databricks Folder Permissions

## References to appropriate REST Apis

1. <a href="https://docs.databricks.com/dev-tools/api/latest/workspace.html" target="_blank"> Workspace Api </a>: Used
downloading and uploading notebooks via terraform provider
2. <a href="https://docs.databricks.com/dev-tools/api/latest/permissions.html" target="_blank"> Permissions Api </a>: Used
exporting permissions on notebooks and folders and then applied to target workspace via terraform provider

## Known Limitations

{{< bootstrap-alert icon="💡" text=`Currently there is only one limitation based on the behavior of the provider. The difference is determined by changes in
the source workspace. So everytime you export if it determines a difference in the checksum of the source file it will determine
that as a change. Any changes made or caused in the target workspace will not be tracked unless the file itself is removed.
` >}}


## Permissions Support

All permissions are dependant users and groups being created successfuly if you are exporting [`identity`]({{< relref "workspace-identities.md" >}}).

Currently the following permissions are exported:
1. Permissions directly attached to the notebooks
2. Permissions associated with any of the folders that are being exported. These folder permissions
   have a dependency on at least one notebook in that folder path being created in the workspace so the folder
   can also exist. This dependancy can be identified in the file itself

## Cloud Specific Behavior

## Export Details

### Example Config

### Config Options

### Exported content + layout

## Import Details

### Environment Variables

### How are increments determined

## Additional Guidance

### Migrations

### Disaster Recovery
