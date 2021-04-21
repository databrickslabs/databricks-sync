## Frequently Asked Questions
* [Troubleshooting](https://github.com/databrickslabs/databricks-sync/blob/master/docs/faq.md#troubleshooting)

### Troubleshooting

---

#### How do I resolve Terraform binary not being located when running `export` or `import` commands?

Example Error Output:
```bash
[INFO] command: terraform --version
[ERROR] cat: /root/.tfenv/version: No such file or directory
[ERROR] Version could not be resolved (set by /root/.tfenv/version or tfenv use <version>) Traceback (most recent call last): File "/usr/local/bin/databricks-sync", line 8, in <module> sys.exit(cli())
```

* If no default version is set or multiple are available, then you will need to set the desired active version using `tfenv use <version>`. This will place the binary of the respective version in the Binaries folder.
* For Terraform binaries installed without `tfenv`, they will need to be placed in the Bin directory.
---

### Usage

---

#### Does Databricks Sync support running commands on the Databricks File System (DBFS) by specifying the root level directory `dbfs:/`?

* Databricks Sync does not support `export` or `import` at the root level of DBFS; however, objects stored on DBFS are supported if a path to the object is specified.
* A [Support Matrix for Import and Export Operations](https://github.com/databrickslabs/databricks-sync#support-matrix-for-import-and-export-operations) is provided that lists supported components of a Databricks workspace.

---
