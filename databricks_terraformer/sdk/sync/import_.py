import functools
import json
import tempfile
from pathlib import Path
from typing import List

from databricks_terraformer.sdk.git_handler import RemoteGitHandler
from databricks_terraformer.sdk.sync.constants import ENTRYPOINT_MAIN_TF
from databricks_terraformer.sdk.terraform import ImportStage, Terraform


def setup_empty_stage(func):
    def __get_stage_path(base_path) -> Path:
        return base_path / "stage"

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        self_ = args[0]
        base_path = Path(self_.tmp_stage.name)
        stage_path = __get_stage_path(base_path)
        resp = func(self_, stage_path=stage_path, **kwargs)
        return resp

    return wrapper


def setup_repo(func):
    def __get_repo_path(base_path) -> Path:
        return base_path / "git-repo"

    def __get_git_repo_path(local_git_path: str, git_ssh_url: str, revision, tmp_stage: Path) -> Path:
        assert any([local_git_path, git_ssh_url]) is True, "atleast local git path or git ssh url should be provided " \
                                                           "otherwise if both are provided it will use local git path"
        if git_ssh_url is not None:

            repo_path = __get_repo_path(tmp_stage)
            RemoteGitHandler(git_ssh_url, repo_path, revision=revision)
            return repo_path
        else:
            return Path(local_git_path)

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        self_ = args[0]
        base_path = Path(self_.tmp_stage.name)
        repo_path = __get_git_repo_path(self_.local_git_path, self_.git_ssh_url,
                                        revision=self_.revision, tmp_stage=base_path)
        resp = func(self_, repo_path=repo_path, **kwargs)
        return resp

    return wrapper


class TerraformExecution:
    def __init__(self, folders: List[str], refresh: bool = True, revision: str = None, plan: bool = False,
                 plan_location: Path = None, state_location: Path = None, apply: bool = False, destroy: bool = False,
                 git_ssh_url: str = None, local_git_path=None):

        self.tmp_stage = tempfile.TemporaryDirectory()
        self.local_git_path = local_git_path
        self.revision = revision
        self.folders = folders
        self.git_ssh_url = git_ssh_url
        self.state_location = state_location
        self.destroy = destroy
        self.refresh = refresh
        self.apply = apply
        self.plan_location = plan_location
        self.plan = plan

    def __del__(self):
        self.tmp_stage.cleanup()

    def validate(self):
        if self.apply and self.destroy:
            raise ValueError("apply and destroy cannot be run at the same time")

    @staticmethod
    def __get_exports_path(base_path) -> Path:
        return base_path / "exports"

    def __stage_all_json_files(self, stage_path, repo_path):
        istg = ImportStage(stage_path)
        for folder in self.folders:
            istg.stage_files(self.__get_exports_path(repo_path) / folder)
        istg.stage_file(self.__get_exports_path(repo_path) / "mapped_variables.tf.json")
        with (stage_path / "main.tf.json").open("w+") as w:
            w.write(json.dumps(ENTRYPOINT_MAIN_TF))

    @setup_empty_stage
    @setup_repo
    def execute(self, stage_path, repo_path):
        # TODO test for each action, stop and retrun failure as needed
        # setup provider and init
        print("executing")
        stage_path.mkdir(parents=True)
        with (stage_path / "main.tf.json").open("w+") as w:
            w.write(json.dumps(ENTRYPOINT_MAIN_TF))
        tf = Terraform(working_dir=str(stage_path), is_env_vars_included=True)
        tf.version()
        tf.init()
        # if we are not destroying download the git repo and stage tf files
        if not self.destroy:
            self.__stage_all_json_files(stage_path, repo_path)

        if self.plan is True:
            tf.plan(
                refresh=self.refresh,
                output_file=self.plan_location,
                state_file_abs_path=self.state_location)

        if self.apply is True:
            tf.apply(
                refresh=self.refresh,
                plan_file=self.plan_location,
                state_file_abs_path=self.state_location)
