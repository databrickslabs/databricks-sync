from databricks_terraformer.hcl.json_to_hcl import create_resource_from_dict


def print_test(ctx, param, value):
    import click

    type = "cluster"
    name = "my_cluster_cluster-1243424e"
    resource_data = {
        "@block:block_test": {
            "@block:super_nested": {
                "nested_sub_sub": 123
            },
            "hello_world": 123,
            "mappy": {
                "nested_sub_sub_sub": 123,
                "test1234": 123
            },
            "interpolation": {
                "@expr:nested_sub_sub_sub": "var.demo_interpolate_variable",
                "test1234": 123
            }
        },
        "test": 12345,
        "test2": "string ${upper.lib}"
    }

    output = create_resource_from_dict(type, name, resource_data, False)

    click.echo(output)
    ctx.exit()
