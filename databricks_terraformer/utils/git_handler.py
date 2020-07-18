import logging
import os
import tempfile
from typing import Text, List

import git

from databricks_terraformer import log

logging.basicConfig(level=logging.INFO)


class GitHandler:
    def __init__(self, git_url, directory, custom_commit_message=None, ignore_deletes=False):
        self.custom_commit_message = custom_commit_message
        self.ignore_deletes = ignore_deletes
        self.directory = directory
        self.git_url = git_url
        self.files_created = []

    def add_file(self, name, data):
        write_path = os.path.join(self.resource_path, name)
        log.info(f"Writing policy to path {write_path}")
        with open(write_path, "w") as f:
            f.write(data)
        self.files_created.append(name)

    def _remove_unmanaged_files(self):
        deleted_file_paths_to_stage = []
        files_to_delete = self.get_files_delete()
        for file in files_to_delete:
            hcl_to_be_deleted_path = os.path.join(self.resource_path, file)
            log.info(f"Deleting policy in path {hcl_to_be_deleted_path}")
            os.remove(hcl_to_be_deleted_path)
            deleted_file_paths_to_stage.append(hcl_to_be_deleted_path)

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

    def get_files_delete(self) -> (List[Text]):
        remote_set = set(self.files_created)
        managed_set = set(os.listdir(self.resource_path))
        return list(managed_set - remote_set)

    def __enter__(self):
        self.local_repo_path = tempfile.TemporaryDirectory()
        self.resource_path = os.path.join(self.local_repo_path.name, self.directory)
        self.repo = self._get_repo()
        os.makedirs(self.resource_path, exist_ok=True)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # If not ignoring deleted remote state, delete all files not explicitly added
        if not self.ignore_deletes:
            self._remove_unmanaged_files()

        # stage all changes
        self._stage_changes()
        # push all changes
        self._push()
        # clean temp folder
        self.local_repo_path.cleanup()

