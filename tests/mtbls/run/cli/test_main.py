import pytest
from click.testing import CliRunner

from mtbls import __version__
from mtbls.run.cli.main import mtbls_tool


@pytest.fixture()
def runner():
    return CliRunner()


def test_mtbls_tool_version(runner):
    result = runner.invoke(mtbls_tool, ["--version"])
    assert result.exit_code == 0
    assert __version__ in result.output.strip()


def test_mtbls_tool_help_01(runner):
    result = runner.invoke(mtbls_tool, ["--help"])
    assert result.exit_code == 0
    assert "validation" in result.output.strip()
