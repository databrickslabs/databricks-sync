from collections import defaultdict
from typing import Dict, Any, List

from pygrok import Grok

from databricks_sync import log


class TerraformValidateError:

    def __init__(self, file, error_msg, content):
        self.content = content
        self.error_msg = error_msg
        self.file = file

    def __repr__(self):
        return str(self.__dict__)


def get_file_name(error_line):
    pattern = "  on %{GREEDYDATA:value} line %{GREEDYDATA}"
    grok = Grok(pattern)
    res: Dict[str, Any] = grok.match(error_line)
    if res is not None and "value" in res:
        return res["value"]
    else:
        return None


def create_errors(error_buffer: "ErrorParserBuffer"):
    for file in error_buffer.file_lines:
        file_name = get_file_name(file)
        if file_name is not None:
            yield TerraformValidateError(file_name, error_buffer.err_msg, error_buffer.err_content)


class ErrorParserBuffer:

    def __init__(self):
        self.file_lines = []
        self.err_content = ""
        self.err_msg = ""

    def __repr__(self):
        return str(self.__dict__)

    def reset(self):
        self.file_lines = []
        self.err_content = ""
        self.err_msg = ""


def fetch_error_blocks(error_content):
    error_block = "Error: "
    warning_block = "Warning: "
    eof = "EOF--synthetic"
    current_block = None
    error_lines = error_content.split("\n") + [eof]
    buffer = []
    start_new_block = False

    for line in error_lines:
        line: str
        if error_block in line or warning_block in line or eof in line:
            start_new_block = True
        # Flush block of error
        if start_new_block is True:
            if current_block == error_block:
                yield buffer
            buffer = []
            start_new_block = False

        buffer.append(line)

        if line.startswith(error_block):
            current_block = error_block
        elif line.startswith(warning_block):
            current_block = warning_block


def parse_tf_validate_errors(error_data):
    file_segment = "  on "
    for error_lines in fetch_error_blocks(error_data):
        if len(error_lines) == 0:
            continue
        error_buffer = ErrorParserBuffer()
        error_buffer.err_msg = error_lines[0]
        for line in error_lines:
            if line.startswith(file_segment):
                error_buffer.file_lines.append(line)
        error_buffer.err_content = "\n".join(error_lines)
        yield from create_errors(error_buffer)
        error_buffer.reset()

def index_errors(error_generator):
    error_index = defaultdict(list)
    for err in error_generator:
        err: TerraformValidateError
        error_index[err.file] += [err]
    return dict(error_index)


def get_error_paths_and_content(error_data):
    errors_indexed = index_errors(parse_tf_validate_errors(error_data))
    failed_validation_files = []
    failed_validation_summary = []
    failed_validation_content = []
    for error_file, err_obj_list in errors_indexed.items():
        err_obj_list: List[TerraformValidateError]
        failed_validation_files.append(error_file)
        failed_validation_summary.append("\n".join(list(set([err.error_msg for err in err_obj_list]))))
        failed_validation_content.append("\n".join(list(set([err.content for err in err_obj_list]))))
    log.debug(f"Failed Files: {str(failed_validation_files)}")
    return failed_validation_files, failed_validation_summary, failed_validation_content