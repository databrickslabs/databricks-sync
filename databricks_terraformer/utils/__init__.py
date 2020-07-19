def normalize_identifier(identifier):
    return_name=remove_emoji(identifier)
    if identifier[0].isdigit():
        return_name ="_"+identifier

    return re.sub("[^a-zA-Z0-9_]+", "_",return_name)


def handle_block(pool_resource_data, pool, block):
    block_resource_data = {}
    for att in pool[block]:
        block_resource_data[att] = pool[block][att]
    pool_resource_data[f"@block:{block}"] = block_resource_data


def handle_map(pool_resource_data, pool, map):
    block_resource_data = {}
    for att in pool[map]:
        block_resource_data[att] = pool[map][att]
    pool_resource_data[f"{map}"] = block_resource_data


import re


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

    return regrex_pattern.sub(r'' ,text)
