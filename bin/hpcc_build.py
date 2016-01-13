import click

@click.group()
@click.option('--src', type=click.Path(), default="~/HPCC_Platform")
@click.option('--dest', type=click.Path(), default="~/build")
@click.pass_context
def cli(ctx, **kwargs):
    """This is a command line tool for building HPCC
    """
    ctx.obj = kwargs

@cli.command()
@click.pass_context
def platform(ctx):
    click.echo('platform')

@cli.command()
@click.pass_context
def clienttool(ctx):
    click.echo('clienttool')
