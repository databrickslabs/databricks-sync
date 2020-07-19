import logging
import os
import tempfile
from pathlib import Path
from typing import Text, List

import click
import git

from databricks_terraformer import log

logging.basicConfig(level=logging.INFO)


class GitHandler:
    def __init__(self, git_url, directory, custom_commit_message=None, delete_not_found=False, dry_run=False):
        self.dry_run = dry_run
        self.custom_commit_message = custom_commit_message
        self.delete_not_found = delete_not_found
        self.directory = directory
        self.git_url = git_url
        self.files_created = []

    def add_file(self, name, data):
        write_path = os.path.join(self.resource_path, name)
        os.makedirs(os.path.dirname(write_path), exist_ok=True)
        log.info(f"Writing policy to path {write_path}")
        with open(write_path, "w") as f:
            f.write(data)
        self.files_created.append(name)

    def _remove_unmanaged_files(self):
        deleted_file_paths_to_stage = []
        files_to_delete = self._get_files_delete()
        for abs_file_path in files_to_delete:
            # hcl_to_be_deleted_path = os.path.join(self.resource_path, file)
            log.info(f"Deleting policy in path {abs_file_path}")
            os.remove(abs_file_path)
            deleted_file_paths_to_stage.append(abs_file_path)

    def _stage_changes(self):
        self.repo.git.add(".")

    def _push(self):
        commit_msg = f"Updated {self.directory} via databricks-terraformer." \
            if self.custom_commit_message is None else self.custom_commit_message
        self.repo.index.commit(commit_msg)
        origin = self.repo.remote()
        origin.push("--no-verify")

    def _get_repo(self):
        try:

            repo = git.Repo.clone_from(self.git_url, self.local_repo_path.name,
                                       branch='master')
        except Exception as e:
            repo = git.Repo(self.local_repo_path.name)
        return repo

    def _get_files_delete(self) -> (List[Text]):
        remote_set = set([os.path.join(self.resource_path, item) for item in list(self.files_created)])
        managed_set = set([str(path.absolute()) for path in list(Path(self.resource_path).rglob("*")) if path.is_file()])
        return list(managed_set - remote_set)

    def _log_diff(self):
        diff = self.repo.git.diff('HEAD', name_status=True)
        if len(diff) is 0:
            log.info("No files were changed and no diff was found.")
        else:
            for line in sorted(diff.split("\n")):
                if line.startswith("D"):
                    click.secho(f"File [A=added|M=modified|D=deleted]: {line}", fg='red')
                if line.startswith("A"):
                    click.secho(f"File [A=added|M=modified|D=deleted]: {line}", fg='green')
                if line.startswith("Y"):
                    click.secho(f"File [A=added|M=modified|D=deleted]: {line}", fg='yellow')

    def __enter__(self):
        self.local_repo_path = tempfile.TemporaryDirectory()
        self.resource_path = os.path.join(self.local_repo_path.name, self.directory)
        self.repo = self._get_repo()
        os.makedirs(self.resource_path, exist_ok=True)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # If not ignoring deleted remote state, delete all files not explicitly added
        if self.delete_not_found is True:
            self._remove_unmanaged_files()

        log.info("===IDENTIFYING AND STAGING GIT CHANGES===")
        # Stage Changes for logging diff
        self._stage_changes()

        # Log Changes
        self._log_diff()

        # Handle Dry Run
        if self.dry_run and self.dry_run is False:
            # push all changes
            self._push()
            log.info("===FINISHED PUSHING CHANGES===")
        else:
            log.info("===RUNNING IN DRY RUN MODE NOT PUSHING CHANGES===")
        # clean temp folder
        self.local_repo_path.cleanup()

