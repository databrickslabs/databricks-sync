import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

from databricks_sync import log


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

    def __init__(self, working_dir: str = None, is_env_vars_included=False):
        self.is_env_vars_included = is_env_vars_included
        self.working_dir = working_dir

    def _cmd(self, cmds, *args, **kwargs):
        capture_output = kwargs.pop('capture_output', True)
        print_output = kwargs.pop("print_output", True)
        # TODO maybe figure out where to set this and how to pass it here
        raise_on_error = True
        if capture_output is True:
            stderr = subprocess.PIPE
            stdout = subprocess.PIPE
        else:
            stderr = sys.stderr
            stdout = sys.stdout

        # cmds = self.generate_cmd_string(cmd, *args, **kwargs)
        log.info('command: {c}'.format(c=' '.join(cmds)))

        working_folder = self.working_dir if self.working_dir is not None else None

        environ_vars = {}
        if self.is_env_vars_included:
            environ_vars = os.environ.copy()

        p = subprocess.Popen(cmds, stdout=stdout, stderr=subprocess.STDOUT,
                             cwd=working_folder, env=environ_vars, close_fds=True)

        output = []
        error = []
        for line in p.stdout:
            content = re.sub(r'\x1b(\[.*?[@-~]|\].*?(\x07|\x1b\\))', '', line.decode("utf-8").rstrip("\n"))
            output.append(content)
            if print_output is True:
                log.info(line.decode("utf-8").rstrip("\n"))

        # for line in p.stderr:
        #     content = re.sub(r'\x1b(\[.*?[@-~]|\].*?(\x07|\x1b\\))', '', line.decode("utf-8").rstrip("\n"))
        #     error.append(content)
        #     log.error(line.decode("utf-8").rstrip("\n"))

        p.communicate()
        # Close buffers
        try:
            p.stdout.flush()
        except:
            pass
        p.stdout.close()
        ret_code = p.returncode
        if capture_output is True:
            out = "\n".join(output)
            err = "\n".join(error)
        else:
            out = None
            err = None

        if ret_code != 0 and raise_on_error:
            raise TerraformCommandError(
                ret_code, ' '.join(cmds), out=out, err=out)

        return ret_code, out, err

    def version(self):
        version_cmd = self.BASE_COMMAND + ["--version"]
        return self._cmd(version_cmd)

    def init(self):
        version_cmd = self.BASE_COMMAND + ["init"]
        return self._cmd(version_cmd)

    def validate(self):
        validate_cmd = self.BASE_COMMAND + ["validate"]
        return self._cmd(validate_cmd)

    @staticmethod
    def is_import_lock():
        return os.getenv("DATABRICKS_SYNC_IMPORT_LOCK", "false").lower()

    @staticmethod
    def get_import_plan_parallelism():
        val = os.getenv("DATABRICKS_SYNC_IMPORT_PLAN_PARALLELISM", -1)
        if isinstance(val, int):
            return val
        else:
            return int(val)

    @staticmethod
    def get_import_apply_parallelism():
        val = os.getenv("DATABRICKS_SYNC_IMPORT_APPLY_PARALLELISM", -1)
        if isinstance(val, int):
            return val
        else:
            return int(val)

    def plan(self, output_file: Path = None, targets=None, state_file_abs_path: Path = None, refresh=None):
        plan_cmd = self.BASE_COMMAND + ["plan"]
        plan_cmd += [f"-lock={self.is_import_lock()}"]
        if self.get_import_plan_parallelism() > 0:
            plan_cmd += [f"-parallelism={str(self.get_import_plan_parallelism())}"]
        if output_file is not None:
            plan_cmd += ["-out", str(output_file.absolute())]
        if state_file_abs_path is not None:
            plan_cmd += ["-state", str(state_file_abs_path.absolute())]
        if refresh is not None and refresh is False:
            plan_cmd += ["-refresh=false"]
        if targets is not None:
            plan_cmd += targets
        plan_cmd += ["-input=false"]
        return self._cmd(plan_cmd)

    def apply(self, plan_file: Path = None, state_file_abs_path: Path = None, refresh=None):
        apply_cmd = self.BASE_COMMAND + ["apply"]
        apply_cmd += [f"-lock={self.is_import_lock()}"]
        if self.get_import_apply_parallelism() > 0:
            apply_cmd += [f"-parallelism={str(self.get_import_apply_parallelism())}"]
        if state_file_abs_path is not None:
            apply_cmd += ["-state", str(state_file_abs_path.absolute())]
        if refresh is not None and refresh is False:
            apply_cmd += ["-refresh=false"]
        if plan_file is not None:
            apply_cmd += [str(plan_file.absolute())]
        return self._cmd(apply_cmd)

    def state_pull(self, state_file_abs_path: Path = None):
        apply_cmd = self.BASE_COMMAND + ["state"]
        if state_file_abs_path is not None:
            apply_cmd += ["-state", str(state_file_abs_path.absolute())]
        apply_cmd += ["pull"]
        return self._cmd(apply_cmd, print_output=False)

    def raw_cmd(self, command):
        return self._cmd(command.split(" "))
