import subprocess

from jinja2 import Template
import re

from databricks_cli.configure.provider import get_config_for_profile
from databricks_cli.sdk import ApiClient


def deEmojify(text):
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

    return regrex_pattern.sub(r'',text)


def genTFValidName(resource_name):
    return_name=deEmojify(resource_name)
    if resource_name[0].isdigit():
        return_name ="_"+resource_name

    # for char in ' ./():-':
    #     return_name = return_name.replace(char,'_')
    return re.sub("[^a-zA-Z0-9_]+", "_",return_name)

def genProvider(workdir):
    with open(workdir + 'provider.tf','w') as outfile:
        outfile.write("""
                provider "databricks" {
                  config_file = "~/.databrickscfg"
                  profile     = "demo"
                }
             """)
        outfile.close()

template_string = """resource "{{ resource_type }}" "{{ resource_name }}_{{ resource_id }}" {
    {%- for key, value in attribute_map.items() %}
    {% if value == True or value == False %}{{ key }} = {{ value|lower }}
    {% elif value is iterable and value is not string %}{%- for value2 in value %} {{ key }} = ["{{ value2 }}"]{% endfor -%}
    {% elif key in comment_attributes %} #{{ key }} = "{{ value }}"
    {% else %}{{ key }} = "{{ value|escape }}"{% endif -%}
    {% endfor -%}
    {%- for block in blocks -%}
    {{ block }}
    {%- endfor %}
}
"""

core_resource_blocks = {
    "flat_map": """
    {{property_name}} ={
        {%- for key, value in attributes.items() %}
        "{{ key }}" = "{{ value }}"
        {%- endfor %}
    }
    """,
    "flat_block": """
    {{property_name}} {
        {%- for key, value in attributes.items() %}
        {{ key }} = "{{ value|escape }}"
        {%- endfor %}
    }
    """,
    "init_scripts": """
    {% for script in init_scripts -%}
    init_scripts {
            {% if script.dbfs %}dbfs {
                destination = "{{ script.dbfs.destination }}"
            }{%- endif %}
            {%- if script.s3 %}s3 {
                    {%- for key, value in script.s3.items() %}
                    {{ key }} = "{{ value }}"
                    {%- endfor %}
           }{%- endif %}
    }
    {% endfor -%}
    """,
    "2dim_block": """
    {{property_name}} {
        {%- for key, value in attributes.items() %}
            {%- if value is not string %}
                {{ key }} {
                    {%- for key2, value2 in value.items() %}
                    {{ key2 }} = "{{ value2 }}"
                    {%- endfor %}
                }
            {% else %}
                {{ key }} = "{{ value }}"
            {%- endif %}
        {%- endfor %}
    }
    """,
    "flat_block_in_array": """
    {% for attributes in array -%}
        {%- for key, value in attributes.items() %}
        {%- for key2, value2 in value.items() %}        
            {{key}}_{{key2}} {
            {%- for key3, value3 in value2.items() %}
                {{ key3 }} = "{{ value3 }}"
            {%- endfor %}
        {%- endfor %}
        {%- endfor %}
    }
    {%- endfor %}
        """,
    "nested_array": """
     {% for script in init_scripts -%}
     init_scripts {
             {% if script.dbfs %}dbfs {
                 destination = "{{ script.dbfs.destination }}"
             }{%- endif %}
             {% if script.s3 %}s3 ={
             destination = "{{ script.s3.destination }}"
             {% if script.s3.region %}region = "{{ script.s3.region }}"{%- endif -%}
             {% if script.s3.endpoint %}endpoint = "{{ script.s3.endpoint }}"{%- endif -%}
             {% if script.s3.enable_encryption %}enable_encryption = "{{ script.s3.enable_encryption }}"{%- endif -%}
             {% if script.s3.encryption_type %}encryption_type = "{{ script.s3.encryption_type }}"{%- endif -%}
             {% if script.s3.kms_key %}kms_key = "{{ script.s3.kms_key }}"{%- endif -%}
             {% if script.s3.canned_acl %}canned_acl = "{{ script.s3.canned_acl }}"{%- endif -%}
            }{%- endif %}
     }
     {% endfor %}
     """,
    "complex_struct": """ {{ resource_name }} {
    {%- for key, value in attribute_map.items() %}
    {% if value == True or value == False %}{{ key }} = {{ value|lower }}
    {% elif value is iterable and value is not string %}{%- for value2 in value %} {{ key }} = ["{{ value2 }}"]{% endfor -%}
    {% elif key in comment_attributes %} #{{ key }} = "{{ value }}"
    {% else %}{{ key }} = "{{ value|escape }}"{% endif -%}
    {% endfor -%}
    {%- for block in blocks -%}
    {{ block }}
    {%- endfor %}
}
"""

}


class AWSAttributes:
    def __init__(self, attribute_map, blocks):
        self.attribute_map = attribute_map
        self.template = Template(core_resource_blocks["flat_block"])
        self.blocks = blocks

    @staticmethod
    def parse(input_dictionary):
        return AWSAttributes(input_dictionary, None)

    def render(self):
        return self.template.render(property_name="aws_attributes", attributes=self.attribute_map)

api_client = None

def get_client(profile='demo'):
    global api_client
    if api_client is None:
        config = get_config_for_profile(profile)
        api_client = ApiClient(host=config.host, token=config.token)

    return api_client


def clean_product(working_dir):
    exec_print_output(False,True,'rm', working_dir + "*")


def exec_print_output(only_error=False, shell=False, *argv):

    ret,process = exec(shell,argv)

    if only_error and ret != 0:
        print(process.stderr)
    elif process is not None:
        print(process.stdout)


def exec(shell=False,*argv):

    if shell:
        process = subprocess.call(' '.join(argv[0][0:]),shell=True)
        return process,None

    else:
        print(' '.join(argv[0]))
        process = subprocess.run(argv[0],
                                 stdout=subprocess.PIPE,
                                 universal_newlines=True)
        process

    return process.returncode,process