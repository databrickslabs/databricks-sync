import copy
import fnmatch

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


class PathExclusion(object):
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
        matched_patterns = []
        match_results = []
        for ex_path in self.__exclude_paths:
            matched = fnmatch.fnmatch(path, ex_path)
            match_results.append(matched)
            if matched is True:
                matched_patterns.append(ex_path)

        is_excluded = any(match_results)
        if is_excluded is True:
            log.debug(f"{self.__resource_type}: {path} matched the following exclusion patterns: {matched_patterns} "
                      f"from the full set of: {self.__exclude_paths}")
        return is_excluded
