import sys

import click

from mtbls.run.cli.validation.create_input_json import create_input_json_cli


@click.group(
    name="validation", context_settings={"help_option_names": ["-h", "--help"]}
)
def validation_group():
    """MetaboLights Validation CLI tool with subcommands."""
    pass


validation_group.add_command(create_input_json_cli)

if __name__ == "__main__":
    if len(sys.argv) == 1:
        validation_group(["--help"])
    else:
        validation_group()
