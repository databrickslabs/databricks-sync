from databricks_terraformer.utils.git_handler import GitExportHandler
import os
import glob

def destroy_all(git_ssh_url,dry_run):

    with GitExportHandler(git_ssh_url, "jobs", dry_run=dry_run) as gh:
        print(gh.local_repo_path.name)
        for file in glob.glob(f"{gh.local_repo_path.name}/**/*.tf",recursive=True):
            if dry_run:
                print(f"Will remove file {file}")
            else:
                os.unlink(file)


#destroy_all('git@github.com:itaiw/export-repo.git',dry_run=True)
