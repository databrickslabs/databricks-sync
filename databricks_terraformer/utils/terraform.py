import ntpath
import os
import subprocess
import sys
import tempfile
from shutil import copyfile
from typing import Optional, List, Text

import git

from databricks_terraformer import log
from databricks_terraformer.utils import TFGitResource


class TerraformCommandError(subprocess.CalledProcessError):
    def __init__(self, ret_code, cmd, out, err):
        super(TerraformCommandError, self).__init__(ret_code, cmd)
        self.out = out
        self.err = err


class Terraform:
    BASE_COMMAND = ["terraform"]

    def __init__(self, working_dir=None, is_env_vars_included=False):
        self.is_env_vars_included = is_env_vars_included
        self.working_dir = working_dir

    def cmd(self, cmds, *args, **kwargs):
        """
        run a terraform command, if success, will try to read state file
        :param cmd: command and sub-command of terraform, seperated with space
                    refer to https://www.terraform.io/docs/commands/index.html
        :param args: arguments of a command
        :param kwargs:  any option flag with key value without prefixed dash character
                if there's a dash in the option name, use under line instead of dash,
                    ex. -no-color --> no_color
                if it's a simple flag with no value, value should be IsFlagged
                    ex. cmd('taint', allow＿missing=IsFlagged)
                if it's a boolean value flag, assign True or false
                if it's a flag could be used multiple times, assign list to it's value
                if it's a "var" variable flag, assign dictionary to it
                if a value is None, will skip this option
                if the option 'capture_output' is passed (with any value other than
                    True), terraform output will be printed to stdout/stderr and
                    "None" will be returned as out and err.
                if the option 'raise_on_error' is passed (with any value that evaluates to True),
                    and the terraform command returns a nonzerop return code, then
                    a TerraformCommandError exception will be raised. The exception object will
                    have the following properties:
                      returncode: The command's return code
                      out: The captured stdout, or None if not captured
                      err: The captured stderr, or None if not captured
        :return: ret_code, out, err
        """
        capture_output = kwargs.pop('capture_output', True)
        raise_on_error = kwargs.pop('raise_on_error', False)
        if capture_output is True:
            stderr = subprocess.PIPE
            stdout = subprocess.PIPE
        else:
            stderr = sys.stderr
            stdout = sys.stdout

        # cmds = self.generate_cmd_string(cmd, *args, **kwargs)
        log.info('command: {c}'.format(c=' '.join(cmds)))

        working_folder = self.working_dir if self.working_dir else None

        environ_vars = {}
        if self.is_env_vars_included:
            environ_vars = os.environ.copy()

        p = subprocess.Popen(cmds, stdout=stdout, stderr=stderr,
                             cwd=working_folder, env=environ_vars)

        synchronous = kwargs.pop('synchronous', True)
        if not synchronous:
            return p, None, None

        out, err = p.communicate()
        ret_code = p.returncode
        log.debug('output: {o}'.format(o=out))

        if capture_output is True:
            out = out.decode('utf-8')
            err = err.decode('utf-8')
        else:
            out = None
            err = None

        if ret_code != 0 and raise_on_error:
            raise TerraformCommandError(
                ret_code, ' '.join(cmds), out=out, err=err)

        return ret_code, out, err

    def version(self):
        version_cmd = self.BASE_COMMAND + ["--version"]
        return self.cmd(version_cmd)

    def init(self):
        version_cmd = self.BASE_COMMAND + ["init"]
        return self.cmd(version_cmd)

    def plan(self, output_file=None, targets=None, state_file_abs_path=None):
        plan_cmd = self.BASE_COMMAND + ["plan"]
        if output_file is not None:
            plan_cmd += ["-out", output_file]
        if state_file_abs_path is not None:
            plan_cmd += ["-state", state_file_abs_path]
        if targets is not None:
            plan_cmd += targets
        return self.cmd(plan_cmd)

    def apply(self, plan_file=None, state_file_abs_path=None):
        apply_cmd = self.BASE_COMMAND + ["apply"]
        if state_file_abs_path is not None:
            apply_cmd += ["-state", state_file_abs_path]
        if plan_file is not None:
            apply_cmd += [plan_file]
        return self.cmd(apply_cmd)


class GitTFStage_V2:

    # TODO: Support tag and batch id for batched-targeted plans
    def __init__(self, git_url, directories: List[Text], cur_ref, artifact_dir,
                 prev_ref=None, init=True, backend_file=None):
        self.backend_file = backend_file
        self.prev_ref = prev_ref
        self.cur_ref = cur_ref
        self.directories = directories
        self.git_url = git_url
        self.artifact_dir = artifact_dir
        self.init = init
        self._terraform: Optional[Terraform] = None

    def _stage_file(self, abs_file_path):
        file_name = ntpath.basename(abs_file_path)
        copyfile(abs_file_path, os.path.join(self.tf_stage_directory.name, file_name))

    # Currently not supporting resource targeting
    # Will eventually set up a resource targetting Mode
    def plan(self):
        state_abs_path = os.path.join(self.artifact_dir, "terraform.tfstate")
        plan_abs_path = os.path.join(self.artifact_dir, "plan.out")
        return_code, stdout, stderr = self._terraform.plan(plan_abs_path,
                                                           state_file_abs_path=state_abs_path)
        log.info(stdout)
        if return_code != 0:
            log.error(stderr)

    def apply(self, custom_plan_path=None):
        state_abs_path = os.path.join(self.artifact_dir, "terraform.tfstate")
        plan_abs_path = os.path.join(self.artifact_dir, "plan.out")
        if custom_plan_path is not None:
            return_code, stdout, stderr = self._terraform.apply(custom_plan_path,
                                                                state_file_abs_path=state_abs_path)
        else:
            return_code, stdout, stderr = self._terraform.apply(plan_abs_path,
                                                                state_file_abs_path=state_abs_path)
        log.info(stdout)
        if return_code != 0:
            log.error(stderr)

    def _init(self):
        if self._terraform is not None:
            self._terraform.init()
        else:
            raise ValueError("Terraform is not configured")

    def _add_provider_block(self):
        provider_path = os.path.join(self.tf_stage_directory.name, "provider.tf")
        with open(provider_path, "w") as f:
            f.write("provider databricks {}")

    def _add_output_tag_block(self):
        provider_path = os.path.join(self.tf_stage_directory.name, "output_commit.tf")
        with open(provider_path, "w") as f:
            f.write("""
            output "cur_git_rev" {{
                value = "{}"
            }}
            output "cur_git_url" {{
                value = "{}"
            }}
            """.format(self.cur_ref, self.git_url))

    def _add_back_end_file(self):
        provider_path = os.path.join(self.tf_stage_directory.name, "backend.tf")
        with open(provider_path, "w") as f:
            with open(self.backend_file, "r") as bk:
                bkend_content = bk.read()
            f.write(bkend_content)
            f.flush()

    def _get_code(self):
        self.local_repo_directory = tempfile.TemporaryDirectory()
        # self.resource_path = os.path.join(self.local_repo_directory.name, self.directory)
        self.repo = git.Repo.clone_from(self.git_url, self.local_repo_directory.name,
                                        branch='master')
        self.repo.git.checkout(self.cur_ref)

    def _git_diff(self):
        if self.prev_ref is None or self.cur_ref is None:
            return None
        fmt = '--name-status'
        commits = []
        differ = self.repo.git.diff(f"{self.prev_ref}..{self.cur_ref}", fmt).split("\n")
        for line in differ:
            if len(line) > 0:
                commits.append(line)
        return commits

    def _identify_files_to_stage(self):
        paths = []
        for d in self.directories:
            path = os.path.join(self.local_repo_directory.name, d)
            for root, d_names, f_names in os.walk(path):
                for f in f_names:
                    paths.append(os.path.join(root, f))
            # for file in os.listdir(path):
            #     # tf_git_resource = TFGitResource.from_file_path(file)
            #     # if tf_git_resource is not None:
            #     paths.append(os.path.join(path, file))

        self.targeted_files_abs_paths = paths

    def __enter__(self):
        # Get the terraform code
        self._get_code()
        # identify targets and changes
        self._identify_files_to_stage()

        # stage the terraform files
        self.tf_stage_directory = tempfile.TemporaryDirectory()

        self._add_provider_block()
        log.info(f"staged provider")

        self._add_output_tag_block()
        log.info(f"added provider")

        if self.backend_file is not None:
            self._add_back_end_file()
            log.info(f"added backend")

        for file_path in self.targeted_files_abs_paths:
            self._stage_file(file_path)
        log.info("copied files")

        contents = os.listdir(self.tf_stage_directory.name)
        log.info(f"TF contents: {contents}")

        self._terraform = Terraform(self.tf_stage_directory.name, True)

        if self.init is True:
            log.info("RUNNING TERRAFORM INIT")
            self._init()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.tf_stage_directory.cleanup()
        self.local_repo_directory.cleanup()
