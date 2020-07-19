def normalize_identifier(identifier):
    return identifier.replace(" ", "_")\
        .replace(".", "_")\
        .replace("/", "_")
