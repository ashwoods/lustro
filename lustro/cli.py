import click

from .db import Mirror
from .utils import oracle_qualified_dsn


@click.group()
@click.option('--source', help="Source DB DSN.")
@click.option('--target', help="Target DB DSN.")
@click.option('--tables', default=None, help="Commna separated tables names to act on.")
@click.option('--source-schema', default=None, help="Source schema/owner")
@click.pass_context
def cli(ctx, source, target, tables, source_schema):
    if ctx.obj is None:
        ctx.obj = {}
    if source.startswith('oracle'):
        source = oracle_qualified_dsn(source)
    ctx.obj['MIRROR'] = Mirror(source=source, target=target, source_schema)
    ctx.obj['TABLES'] = tables


@cli.command()
@click.pass_context
def create(ctx):
    """Creates the schema but doesn't copy any data"""
    mirror = ctx.obj['MIRROR']
    mirror.create(tables=ctx.obj['TABLES'])


@cli.command()
@click.option('--modified', default='modified', help='Specify name of modified field')
@click.pass_context
def diff(ctx, modified):
    """Creates the schema if it doesn't exist and copies """
    mirror = ctx.obj['MIRROR']
    mirror.diff(tables=ctx.obj['TABLES'], modified=modified)


@cli.command()
@click.pass_context
def recreate(ctx):
    mirror = ctx.obj['MIRROR']
    mirror.recreate(tables=ctx.obj['TABLES'])


@cli.command()
@click.pass_context
def mirror(ctx):
    mirror = ctx.obj['MIRROR']
    mirror.mirror(tables=ctx.obj['TABLES'])
