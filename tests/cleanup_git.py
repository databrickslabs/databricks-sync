import shutil
from pathlib import Path
import tempfile
from git import Repo


from databricks_terraformer import log

import os
import glob

def destroy_all(git_url,dry_run):
    with tempfile.TemporaryDirectory() as tmp:
        base_path = Path(tmp)

        repo = Repo.clone_from(git_url, base_path.absolute(),
                               branch='master')

        for file in glob.glob(f"{base_path.absolute()}/*",recursive=True):
            print(file)
            if dry_run is False:
                repo.git.rm(file, r=True)
            else:
                log.info(f"Will remove file {file}")

        commit_msg = "Deleted via databricks-sync."
        repo.index.commit(commit_msg)
        origin = repo.remote()
        origin.push("--no-verify")





# destroy_all('git@github.com:itaiw/export-repo.git',dry_run=False)
