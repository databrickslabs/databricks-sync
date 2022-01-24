from pkg_resources import get_distribution, DistributionNotFound


def get_version():
    try:
        return get_distribution("databricks-sync").version
    except DistributionNotFound:
        return "unknown"



def print_version_callback(ctx, param, value):
    import click
    if not value or ctx.resilient_parsing:
        return
    click.echo('Version {}'.format(get_version()))
    ctx.exit()
