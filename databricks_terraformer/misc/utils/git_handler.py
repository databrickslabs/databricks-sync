import datetime
import logging
import ntpath
import os
import tempfile
from pathlib import Path
from typing import Text, List

import click
import git

from databricks_terraformer import log
from databricks_terraformer.misc.utils import create_change_log, get_previous_changes

logging.basicConfig(level=logging.INFO)


class GitExportHandler:
    def __init__(self, git_url, directory, custom_commit_message=None, delete_not_found=False, dry_run=False,
                 tag=False):
        self.tag = tag
        self._tag_now = datetime.datetime.now()
        self._tag_value = self._get_now_as_tag(self._tag_now)
        self.dry_run = dry_run
        self.custom_commit_message = custom_commit_message
        self.delete_not_found = delete_not_found
        self.directory = directory
        self.git_url = git_url
        self.files_created = []
        self._git_added = []
        self._git_modified = []
        self._git_removed = []
        self._ignore_remove_files = ["README.md"]

    def add_file(self, name, data):
        write_path = os.path.join(self.resource_path, name)
        os.makedirs(os.path.dirname(write_path), exist_ok=True)
        log.info(f"Writing {self.directory} to path {write_path}")
        with open(write_path, "w") as f:
            f.write(data)
        self.files_created.append(name)

    def _remove_unmanaged_files(self):
        deleted_file_paths_to_stage = []
        files_to_delete = self._get_files_delete()
        for abs_file_path in files_to_delete:
            # Skip ignored files
            file_name = ntpath.basename(abs_file_path)
            if file_name not in self._ignore_remove_files:
                log.info(f"Deleting {self.directory} in path {abs_file_path}")
                os.remove(abs_file_path)
                deleted_file_paths_to_stage.append(abs_file_path)

    def _create_tag(self):
        self._git_tag = self.repo.create_tag(self._tag_value, message=f'Updated {self.directory} "{self._tag_value}"')

    def _stage_changes(self):
        self.repo.git.add(A=True)

    def _push_tags(self):
        if self._git_tag is not None:
            self.repo.git.push("origin", self._tag_value, "--porcelain", "--no-verify")
        else:
            log.error("Unable to find the tag")

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
        managed_set = set(
            [str(path.absolute()) for path in list(Path(self.resource_path).rglob("*")) if path.is_file()])
        return list(managed_set - remote_set)

    def _log_diff(self):
        diff = self.repo.git.diff('HEAD', name_status=True)
        if len(diff) is 0:
            log.info("No files were changed and no diff was found.")
        else:
            for line in sorted(diff.split("\n")):
                # get the relative path of the file relative to the handler directory
                rel_path = line.split('\t')[1].replace(self.directory + "/", "")
                if line.startswith("D"):
                    self._git_removed.append(rel_path)
                    click.secho(f"File [A=added|M=modified|D=deleted]: {line}", fg='red')
                if line.startswith("A"):
                    self._git_added.append(rel_path)
                    click.secho(f"File [A=added|M=modified|D=deleted]: {line}", fg='green')
                if line.startswith("M"):
                    self._git_removed.append(rel_path)
                    click.secho(f"File [A=added|M=modified|D=deleted]: {line}", fg='yellow')

    def _get_now_as_tag(self, now):
        now_str = now.strftime("%Y%m%d%H%M%S%f")
        return f"v{now_str}"

    def _create_or_update_change_log(self):
        previous_changes = get_previous_changes(self.resource_path)
        ch_log = create_change_log(self.directory, self._tag_value, self._tag_now,
                                   added_files=self._git_added, modified_files=self._git_modified,
                                   base_path=self.resource_path,
                                   removed_files=self._git_removed, previous=previous_changes)
        log.debug(f"Generated changelog: \n{ch_log}")

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

        self._stage_changes()

        # First differences need to be logged before applying change log
        self._create_or_update_change_log()

        # Stage stage the change log TODO: maybe this should be a decorator
        self._stage_changes()

        # Handle Dry Run
        if self.dry_run is not None and self.dry_run is False:
            # push all changes
            self._push()
            log.info("===FINISHED PUSHING CHANGES===")

            # Tag and push previous changes
            if self.tag is True:
                # Create tag
                self._create_tag()
                # push the tag
                self._push_tags()
                log.info(f"===FINISHED PUSHING TAG {self._tag_value}===")

        else:
            log.info("===RUNNING IN DRY RUN MODE NOT PUSHING CHANGES===")
        # clean temp folder
        self.local_repo_path.cleanup()
