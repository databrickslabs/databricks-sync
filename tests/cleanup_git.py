import glob
import tempfile
from pathlib import Path

from git import Repo

from databricks_sync import log


def destroy_all(git_url, dry_run, branch='master'):
    with tempfile.TemporaryDirectory() as tmp:
        base_path = Path(tmp)

        repo = Repo.clone_from(git_url, base_path.absolute(),
                               branch=branch)

        for file in glob.glob(f"{base_path.absolute()}/*", recursive=True):
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
