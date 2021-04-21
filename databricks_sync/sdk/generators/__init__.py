import copy
import fnmatch
import os
from typing import List

from databricks_sync import log


def drop_all_but(dictionary, *fields, dictionary_name=None):
    result = copy.deepcopy(dictionary)
    invalid_keys = []
    for key in result:
        if key not in fields:
            log.debug(f"Found key: {key} in {dictionary_name} but it should not be there for terraform.")
            invalid_keys.append(key)
    for invalid_key in invalid_keys:
        result.pop(invalid_key)
    return result

def match_patterns(string, patterns) -> (bool, List[str]):
    matched_patterns = []
    match_results = []
    for ex_path in patterns:
        matched = fnmatch.fnmatch(string, ex_path)
        match_results.append(matched)
        if matched is True:
            matched_patterns.append(ex_path)

    return any(match_results), matched_patterns

class PathInclusionParser(object):

    def __init__(self, path_patterns: List[str], resource_type):
        self.__resource_type = resource_type
        self.__path_patterns = [self.__add_implicit_recursive_glob(path) for path in path_patterns]
        self.__base_paths = self.get_base_paths()
        # Normalize for dbfs and remove any file system
        self.__all_path_patterns = [pat.lstrip("dbfs:") for pat in list(set(self.__path_patterns + self.__base_paths))]
        # if no glob factor is added to any of the patterns then add a ** for implicit recursion
        # self.__all_path_patterns = [ pat+"**" if "*" not in pat else pat for pat in self.__all_path_patterns]
        self.__processed_paths = set()

    def __add_implicit_recursive_glob(self, path: str):
        if "*" in path:
            return path
        if path.endswith("/"):
            return path + "**"
        else:
            return path + "/**"

    def __path_parts(self, path):
        path_part_list = []
        while True:
            parts = os.path.split(path)
            if parts[0] == path:
                break
            else:
                path = parts[0]
                path_part_list.insert(0, parts[1])
        return path_part_list

    def __get_base_path(self, path):
        parts = self.__path_parts(path.lstrip("dbfs:"))
        actual_path_parts = []
        for part in parts:
            if "*" in part:
                break
            actual_path_parts.append(part)
        # Normalize for dbfs and remove any file system
        if path.startswith("dbfs:"):
            return "dbfs:/" + "/".join(actual_path_parts)
        else:
            return "/"+"/".join(actual_path_parts)

    def get_base_paths(self):
        return [self.__get_base_path(path) for path in self.__path_patterns]

    def __is_path_processed(self, path):
        if path in self.__processed_paths:
            return True
        else:
            self.__processed_paths.add(path)
            return False

    def is_path_included(self, path):
        # If no exclude paths are not defined then skip this step
        if self.__is_path_processed(path) is True:
            log.debug(f"[PathInclusion] Path: {path} has been processed will skip going down this tree.")
            return False

        # Normalize for dbfs and remove any file system
        is_included, matched_patterns = match_patterns(path.lstrip("dbfs:"), self.__all_path_patterns)

        if is_included is True:
            log.debug(f"[PathInclusion] {self.__resource_type}: {path} path matched the following inclusion patterns: "
                      f"{matched_patterns} from the full set of: {self.__all_path_patterns}")
        return is_included

    @property
    def base_paths(self):
        return self.__base_paths

class PathExclusionParser(object):
    def __init__(self, exclude_path, resource_type):
        self.__resource_type = resource_type
        # Properly store exclude paths
        if exclude_path is None:
            self.__exclude_paths = None
        elif isinstance(exclude_path, str):
            self.__exclude_paths = [exclude_path]
        else:
            self.__exclude_paths = exclude_path

    def is_path_excluded(self, path):
        # If no exclude paths are not defined then skip this step
        if self.__exclude_paths is None:
            return False
        # Normalize for dbfs and remove any file system
        is_excluded, matched_patterns = match_patterns(path.lstrip("dbfs:"),
                                                       [pat.lstrip("dbfs:") for pat in self.__exclude_paths])
        if is_excluded is True:
            log.debug(f"[PathExclusion] {self.__resource_type}: {path} path matched the following exclusion patterns: "
                      f"{matched_patterns} from the full set of: {self.__exclude_paths}")
        return is_excluded
