import getpass
import os
import platform

import click

from elastic.simulator import dp_simulation


@click.group()
@click.pass_context
def cli(ctx, **kwargs):
    pass


@cli.command()
@click.option('-m', type=int, default=4)
@click.option('-n', type=int, default=8)
@click.option('-k', type=int, default=1)
@click.option('-t', '--type', type=click.Choice(['mcs', 'mlb', 'rainbow', 'monochromatic']), default='mcs')
@click.option('-w', '--workload', type=click.Choice(['uniform', 'beta', 'normal', 'powerlaw', 'gamma']), default='powerlaw')
@click.option('-f', '--frequency', type=str, default='')
@click.pass_context
def run(ctx, m, n, k, type, workload, frequency):
    if len(frequency) > 0 and ',' in frequency:
        af_list = [float(n.strip()) for n in frequency.split(',')]
        dp_simulation.run(m, n, k, type, None, af_list=af_list)
    else:
        dp_simulation.run(m, n, k, type, workload, af_list=[])