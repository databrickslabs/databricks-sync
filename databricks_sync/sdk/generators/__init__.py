import copy
import fnmatch
from typing import List, Any

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


def listify(val: Any):
    if val is None:
        return []
    if isinstance(val, list):
        return val
    else:
        return [val]


def match_patterns(string, patterns) -> (bool, List[str]):
    matched_patterns = []
    match_results = []
    for ex_path in patterns:
        matched = fnmatch.fnmatch(string, ex_path)
        match_results.append(matched)
        if matched is True:
            matched_patterns.append(ex_path)

    return any(match_results), matched_patterns


def should_i_process(resource_type, value, exclusion_patterns=None, inclusion_patterns=None, resource_short_name=''):
    if exclusion_patterns is not None and isinstance(exclusion_patterns, list) and len(exclusion_patterns) > 0:
        is_excluded, matched_patterns = match_patterns(value, exclusion_patterns)
        if is_excluded is True:
            log.debug(f"[{resource_short_name}Exclusion] {resource_type}: {value} "
                      f"path matched the following exclusion patterns: "
                      f"{matched_patterns} from the full set of: {exclusion_patterns}")
            return False
    if inclusion_patterns is not None and isinstance(inclusion_patterns, list) and len(inclusion_patterns) > 0:
        is_included, matched_patterns = match_patterns(value, inclusion_patterns)

        if is_included is True:
            log.debug(f"[{resource_short_name}Inclusion] {resource_type}: {value} path "
                      f"matched the following inclusion patterns: "
                      f"{matched_patterns} from the full set of: {inclusion_patterns}")
        else:
            return False

    return True
