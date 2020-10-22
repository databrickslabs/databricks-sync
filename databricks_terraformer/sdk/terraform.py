import os
import shutil
import subprocess
import sys
from pathlib import Path

from databricks_terraformer import log


class ImportStage:

    def __init__(self, base_path: Path):
        if not base_path.exists():
            base_path.mkdir(parents=True, exist_ok=True)
        self.__base_path = base_path

    @property
    def stage_dir(self):
        return self.__base_path

    def stage_files(self, from_dir: Path):
        # Does not respect nesting
        for dirpath, dnames, fnames in os.walk(from_dir):
            for f in fnames:
                src_path = os.path.join(dirpath, f)
                shutil.copy(src_path, self.__base_path)

    def stage_file(self, from_path: Path):
        # Does not respect nesting
        if from_path.exists():
            shutil.copy(from_path, self.__base_path)


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
        # TODO maybe figure out where to set this and how to pass it here
        raise_on_error = True
        if capture_output is True:
            stderr = sys.stdout
            stdout = subprocess.PIPE
        else:
            stderr = sys.stderr
            stdout = sys.stdout

        # cmds = self.generate_cmd_string(cmd, *args, **kwargs)
        log.info('command: {c}'.format(c=' '.join(cmds)))
        # print('command: {c}'.format(c=' '.join(cmds)))

        working_folder = self.working_dir if self.working_dir is not None else None

        environ_vars = {}
        if self.is_env_vars_included:
            environ_vars = os.environ.copy()

        p = subprocess.Popen(cmds, stdout=stdout, stderr=stderr,
                             cwd=working_folder, env=environ_vars)

        # synchronous = kwargs.pop('synchronous', True)
        # if not synchronous:
        #     return p, None, None

        for line in p.stdout:
            # print(line.decode("utf-8"), end="")
            log.info(line.decode("utf-8").rstrip("\n"))
            # log.debug('output: {o}'.format(o=line.decode("utf-8")))
        out, err = p.communicate()
        ret_code = p.returncode
        if capture_output is True:
            out = out.decode('utf-8')
            err = None
            # err = err.decode('utf-8')
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

    def validate(self):
        validate_cmd = self.BASE_COMMAND + ["validate"]
        return self.cmd(validate_cmd)

    def plan(self, output_file: Path = None, targets=None, state_file_abs_path: Path = None, refresh=None):
        plan_cmd = self.BASE_COMMAND + ["plan"]
        if output_file is not None:
            plan_cmd += ["-out", str(output_file.absolute())]
        if state_file_abs_path is not None:
            plan_cmd += ["-state", str(state_file_abs_path.absolute())]
        if refresh is not None and refresh is False:
            plan_cmd += ["-refresh=false"]
        if targets is not None:
            plan_cmd += targets

        return self.cmd(plan_cmd)

    def apply(self, plan_file: Path = None, state_file_abs_path: Path = None, refresh=None):
        apply_cmd = self.BASE_COMMAND + ["apply"]
        if state_file_abs_path is not None:
            apply_cmd += ["-state", str(state_file_abs_path.absolute())]
        if refresh is not None and refresh is False:
            apply_cmd += ["-refresh=false"]
        if plan_file is not None:
            apply_cmd += [str(plan_file.absolute())]
        return self.cmd(apply_cmd)
