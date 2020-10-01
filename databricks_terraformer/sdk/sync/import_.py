import functools
import json
import tempfile
from pathlib import Path
from typing import List

from databricks_terraformer.sdk.git_handler import GitHandler
from databricks_terraformer.sdk.terraform import ImportStage, Terraform

entrypoint = {
    "provider": {
        "databricks": {}
    },
    "terraform": {
        "required_version": ">= 0.13.0",
        "required_providers": {
            "databricks": {
                "source": "databrickslabs/databricks",
                # This should be fixed to not impact this tools behavior when downstream changes are made to the
                # RP. This should be consciously upgraded. Maybe in the future can be passed in as optional
                "version": "0.2.5"
            }
        }
    },
}


def setup_empty_stage(func):
    def __get_stage_path(base_path) -> Path:
        return base_path / "stage"

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        _self = args[0]
        with tempfile.TemporaryDirectory() as tmp:
            base_path = Path(tmp)
            stage_path = __get_stage_path(base_path)
            resp = func(_self, stage_path=stage_path, **kwargs)
        return resp

    return wrapper


def setup_repo(func):
    def __get_repo_path(base_path) -> Path:
        return base_path / "git-repo"

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        _self = args[0]
        with tempfile.TemporaryDirectory() as tmp:
            base_path = Path(tmp)
            repo_path = __get_repo_path(base_path)
            GitHandler(_self.git_url, repo_path, revision=_self.revision)
            resp = func(_self, repo_path=repo_path, **kwargs)
        return resp

    return wrapper


class TerraformExecution:
    def __init__(self,
                 git_url: str,
                 folders: List[str],
                 revision: str = None,
                 plan: bool = False,
                 plan_location: Path = None,
                 state_location: Path = None,
                 apply: bool = False,
                 destroy: bool = False,
                 refresh: bool = True):

        self.revision = revision
        self.folders = folders
        self.git_url = git_url
        self.state_location = state_location
        self.destroy = destroy
        self.refresh = refresh
        self.apply = apply
        self.plan_location = plan_location
        self.plan = plan

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
            w.write(json.dumps(entrypoint))

    @setup_empty_stage
    @setup_repo
    def execute(self, stage_path, repo_path):
        # setup provider and init
        stage_path.mkdir(parents=True)
        with (stage_path / "main.tf.json").open("w+") as w:
            w.write(json.dumps(entrypoint))
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
