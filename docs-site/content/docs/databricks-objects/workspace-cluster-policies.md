---
title: "Cluster Policies"
description: "This guide tells you how databricks sync moves Cluster Policies and Cluster Policy Permissions across workspaces."
lead: ""
date: 2020-10-13T15:21:01+02:00
lastmod: 2020-10-13T15:21:01+02:00
draft: false
images: []
menu:
  docs:
    parent: "objects"
weight: 330
toc: true
---

## Impacted Databricks Resources

The impacted resources for this workspace object are:

1. <a href="https://docs.databricks.com/administration-guide/clusters/policies.html" target="_blank"> Databricks Cluster Policies </a>
2. <a href="https://docs.databricks.com/administration-guide/clusters/policies.html#manage-cluster-policy-permissions" target="_blank"> Databricks Cluster Policy Permissions </a>

## References to appropriate REST Apis

1. <a href="https://docs.databricks.com/dev-tools/api/latest/policies.html" target="_blank"> Cluster Policies Api </a>: Used for
downloading and uploading cluster policies via terraform provider
2. <a href="https://docs.databricks.com/dev-tools/api/latest/permissions.html" target="_blank"> Permissions Api </a>: Used
exporting permissions on cluster policies and then applied to target workspace via terraform provider

## Known Limitations

## Permissions Support

## Cloud Specific Behavior

## Export Details

### Example Config

### Config Options

### Handled Dependencies

### Exported content + layout

## Import Details

### Environment Variables

### How are increments determined

## Additional Guidance

### Migrations

### Disaster Recovery
