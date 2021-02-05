import copy

from databricks_terraformer import log


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
