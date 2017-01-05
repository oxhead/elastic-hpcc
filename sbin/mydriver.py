import os
import logging

import click

from elastic import init
from elastic.benchmark.impl.zeromqimpl import *
from elastic.util import daemon


class BenchmarkDriverDaemon(daemon.Daemon):
    def __init__(self, config_path, driver_id):
        super().__init__('/tmp/mydriver_{}.pid'.format(driver_id))
        init.setup_logging(default_level=logging.DEBUG, config_path="conf/logging.yaml", log_dir="logs", component="driver_{}".format(driver_id))
        self.benchmark_driver = BenchmarkDriver(config_path, driver_id)

    def run(self):
        self.benchmark_driver.start()


@click.group()
@click.option('--config', type=click.Path(exists=True, resolve_path=True), default="conf/benchmark.yaml")
@click.pass_context
def cli(ctx, **kwargs):
    """This is a command line tool that works for VCL instances
    """
    ctx.obj = kwargs
    if ctx.obj['config'] is not None:
        ctx.obj['_config'] = BenchmarkConfig.parse_file(ctx.obj['config'])

@cli.command()
@click.argument('driver_id', type=int)
@click.pass_context
def start(ctx, driver_id):
    BenchmarkDriverDaemon(ctx.obj['config'], driver_id).start()


@cli.command()
@click.argument('driver_id', type=int)
@click.pass_context
def stop(ctx, driver_id):
    BenchmarkDriverDaemon(ctx.obj['config'], driver_id).stop()


@cli.command()
@click.argument('driver_id', type=int)
@click.pass_context
def restart(ctx, driver_id):
    BenchmarkDriverDaemon(ctx.obj['config'], driver_id).restart()
