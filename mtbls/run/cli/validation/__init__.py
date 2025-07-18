import sys

import click

from mtbls.run.cli.validation.validate_studies import run_validation_cli


@click.group(name="validation")
def validation_cli():
    """Commands to run validation."""


validation_cli.add_command(run_validation_cli)
if __name__ == "__main__":
    if len(sys.argv) == 1:
        validation_cli(["--help"])
    else:
        validation_cli()
