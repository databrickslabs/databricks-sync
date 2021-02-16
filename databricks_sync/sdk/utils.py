import functools
import re
import urllib
from typing import Dict


def azure_s3_dbfs(data: Dict) -> Dict:
    for key, value in data.items():
        if type(value) == dict:
            for dict_key, value in data.get(key).items():
                return {key.lower().replace('s3', 'dbfs'): {dict_key: re.sub(r's3.*:\/', 'dbfs:/', value.lower())}}
        else:
            return {key: value.lower().replace('s3', 'dbfs')}


def get_azure_path(path: str):
    result = urllib.parse.urlparse(path)
    if result.scheme in ["dbfs", "file"]:
        return path
    else:
        return f"dbfs:/{result.netloc}{result.path}"


def collect_to_list(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return list(func(*args, **kwargs))

    return wrapper


def contains_cloud_specific_path(path):
    url = urllib.parse.urlparse(path)
    return url.scheme == "s3"


def contains_cloud_specific_storage_info(data: Dict):
    for _, value in data.items():
        path = value["destination"]
        return contains_cloud_specific_path(path)


def contains_cloud_specific_library_path(data: Dict):
    for key, value in data.items():
        if key in ["jar", "whl", "egg"]:
            return contains_cloud_specific_path(value)


@collect_to_list
def handle_azure_storage_info(data: Dict):
    # If creating storage info from aws we need to omit s3://path option into DBFS
    # There should be atleast one value for cluster log conf or init scripts
    for key, value in data.items():
        yield {"dbfs": {"destination": get_azure_path(value["destination"])}}


@collect_to_list
def handle_azure_libraries(data: Dict):
    # If creating libraries from aws we need to omit s3://path option into DBFS
    for key, value in data.items():
        if key in ["jar", "whl", "egg"]:
            yield {key: get_azure_path(value)}
        else:
            yield {key: value}


def normalize(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        output = func(*args, **kwargs)
        return normalize_identifier(output)

    return wrapper


def normalize_identifier(identifier):
    return_name = remove_emoji(identifier)
    if identifier[0].isdigit():
        return_name = "_" + identifier

    return re.sub("[^a-zA-Z0-9_]+", "_", return_name)


def remove_emoji(text):
    regrex_pattern = re.compile("["
                                u"\U0001F600-\U0001F64F"  # emoticons
                                u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                                u"\U0001F680-\U0001F6FF"  # transport & map symbols
                                u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                                u"\U00002500-\U00002BEF"  # chinese char
                                u"\U00002702-\U000027B0"
                                u"\U00002702-\U000027B0"
                                u"\U000024C2-\U0001F251"
                                u"\U0001f926-\U0001f937"
                                u"\U00010000-\U0010ffff"
                                u"\u2640-\u2642"
                                u"\u2600-\u2B55"
                                u"\u200d"
                                u"\u23cf"
                                u"\u23e9"
                                u"\u231a"
                                u"\ufe0f"  # dingbats
                                u"\u3030"
                                "]+", flags=re.UNICODE)

    return regrex_pattern.sub(r'', text)
