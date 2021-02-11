import abc
import datetime
import shutil
from pathlib import Path

import git
from git import Repo

from databricks_sync import log


class GitHandler(abc.ABC):

    def __init__(self, base_path: Path, delete_directory: Path = None, branch="master", revision=None):
        self.base_path = base_path
        self.delete_directory = base_path / delete_directory if delete_directory is not None else None
        self.repo = self._get_repo(branch, revision)
        self._handle_gitignore()
        self._delete_directory()

    @abc.abstractmethod
    def _get_repo(self, branch, revision=None) -> git.Repo:
        pass

    def _handle_gitignore(self):
        git_ignore_path = (self.base_path / ".gitignore")
        terraform_ignored = ".terraform" in git_ignore_path.read_text().split("\n") if git_ignore_path.exists() \
            else False
        if terraform_ignored is False:
            with git_ignore_path.open("a+") as f:
                f.write(".terraform\n")
        self.stage_changes()
        self._commit("Databricks Sync committed gitignore changes.")

    def _delete_directory(self):
        if self.delete_directory is not None and self.delete_directory.exists():
            log.info(self.delete_directory.absolute())
            shutil.rmtree(self.delete_directory.absolute())

    # TODO: Uncomment and test when we need capabilities to report on differences
    def get_changes(self, directory):
        # TODO: maybe do this but lets understand what the customer wants to see in regards to report.
        # TODO: Is the log enough on the CI/CD tool
        diff = self.repo.git.diff('HEAD', '--', directory, name_status=True)
        if len(diff) is 0:
            log.info(f"No files were changed and no diff was found in directory/file: {directory}.")
        else:
            log.info(f"Processing changes found in directory/file: {directory}.")
            category_changes = {}
            for line in sorted(diff.split("\n")):
                # get the relative path of the file relative to the handler directory
                rel_path = line.split('\t')[1].replace(directory + "/", "")
                parts = line.split("/")
                if len(parts) in [0, 1, 2]:
                    continue
                category = parts[1]
                if category not in category_changes:
                    category_changes[category] = {
                        "added": [],
                        "modified": [],
                        "deleted": []
                    }
                if line.startswith("D"):
                    category_changes[category]["deleted"].append(rel_path)
                if line.startswith("A"):
                    category_changes[category]["added"].append(rel_path)
                if line.startswith("M"):
                    category_changes[category]["modified"].append(rel_path)
            return category_changes

    def stage_changes(self):
        self.repo.git.add(A=True)

    def _get_now_as_tag(self):
        now = datetime.datetime.now()
        now_str = now.strftime("%Y%m%d%H%M%S%f")
        return f"v{now_str}"

    def _push(self):
        origin = self.repo.remote()
        origin.push("--no-verify")

    def _commit(self, commit_msg="Updated via databricks-sync."):
        self.repo.index.commit(commit_msg)

    def _create_tag(self):
        tag = self._get_now_as_tag()
        self.repo.create_tag(tag, message=f'Updated with tag "{tag}"')
        return tag

    def _push_tags(self, tag_value):
        if tag_value is not None:
            self.repo.git.push("origin", tag_value, "--porcelain", "--no-verify")
        else:
            log.error("Unable to find the tag")

    @abc.abstractmethod
    def commit_and_push(self):
        pass


# Just local git repo
class LocalGitHandler(GitHandler):

    def _get_repo(self, branch, revision=None) -> git.Repo:
        repo = git.Repo.init(self.base_path.absolute())
        log.info("running git init")
        if revision is not None:
            repo.git.checkout(revision)
        return repo

    def commit_and_push(self):
        self._commit()
        log.info("===FINISHED LOCAL COMMIT===")
        tag_value = self._create_tag()
        log.info(f"===FINISHED LOCAL TAGGING: {tag_value}===")


# Should have origin
class RemoteGitHandler(GitHandler):

    def __init__(self, git_ssh_url, base_path: Path, delete_directory: Path = None, branch="master", revision=None):
        self.git_ssh_url = git_ssh_url
        super().__init__(base_path, delete_directory, branch, revision)

    def _get_repo(self, branch, revision=None) -> git.Repo:
        repo = Repo.clone_from(self.git_ssh_url, self.base_path.absolute(),
                               branch=branch)
        if revision is not None:
            repo.git.checkout(revision)
        return repo

    def commit_and_push(self):
        self._commit()
        self._push()
        log.info("===FINISHED PUSHING CHANGES===")
        tag_value = self._create_tag()
        self._push_tags(tag_value)
        log.info(f"===FINISHED PUSHING TAG {tag_value}===")
