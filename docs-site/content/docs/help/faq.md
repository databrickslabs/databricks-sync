---
title: "FAQ"
description: "Answers to frequently asked questions."
lead: "Answers to frequently asked questions."
date: 2020-10-06T08:49:31+00:00
lastmod: 2020-10-06T08:49:31+00:00
draft: false
images: []
menu:
  docs:
    parent: "help"
weight: 600
toc: true
---



### How do I resolve Terraform binary not being located when running `export` or `import` commands?

Example Error Output:
```bash
[INFO] command: terraform --version
[ERROR] cat: /root/.tfenv/version: No such file or directory
[ERROR] Version could not be resolved (set by /root/.tfenv/version or tfenv use <version>) Traceback (most recent call last): File "/usr/local/bin/databricks-sync", line 8, in <module> sys.exit(cli())
```

* Using `tfenv install <version>` will automatically place the Terraform binary in the Bin directory if one version exists. If multiple Terraform versions are available, then you will need to set the desired active version using `tfenv use <version>`, and this will update the Binaries folder.
* For Terraform binaries installed without `tfenv`, they will need to be placed in the Bin directory.
---
