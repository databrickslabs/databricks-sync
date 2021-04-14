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
