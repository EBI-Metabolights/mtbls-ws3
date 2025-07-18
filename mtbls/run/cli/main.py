import click

from mtbls.run.cli.validation import validation_cli


@click.group()
def cli():
    pass


cli.add_command(validation_cli)
