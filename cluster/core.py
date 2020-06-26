from jinja2 import Template

template_string = """

resource "{{ resource_type }}" "{{ resource_name }}_{{ resource_id }}" {
    {%- for key, value in attribute_map.items() %}
    {% if value == True or value == False %}{{ key }} = {{ value|lower }}{% else %}{{ key }} = "{{ value }}"{% endif -%}
    {% endfor -%}
    {%- for block in blocks -%}
    {{ block }}
    {%- endfor %}
}
"""

core_resource_blocks = {
    # "autoscale": """
    # autoscale {
    #          min_workers = {{min_workers}}
    #          max_workers = {{max_workers}}
    #
    #     }
    # """,
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
        {{ key }} = "{{ value }}"
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
    """
}


class AWSAttributes:
    def __init__(self, attribute_map, blocks):
        self.attribute_map = attribute_map
        self.template = Template(core_resource_blocks["flat_block"])
        assert "zone_id" in attribute_map
        assert "availability" in attribute_map
        self.blocks = blocks

    @staticmethod
    def parse(input_dictionary):
        return AWSAttributes(input_dictionary, None)

    def render(self):
        return self.template.render(property_name="aws_attributes", attributes=self.attribute_map)
