# Overview

## MetaboLights WS3

______________________________________________________________________

The `mtbls-ws3` is Python application stack to manage MetaboLights open repository. It provides REST API endpoints for the metabolomics community and other MetaboLights applications.

______________________________________________________________________

## Features

- REST API endpoints with [FastAPI](https://github.com/fastapi/fastapi) and API documentation ([Redoc](https://github.com/Redocly/redoc)).
- Json serializable models with [pydantic](https://github.com/pydantic/pydantic) library.
- Serve the selected REST API endpoints (e.g., submission, curation, public, etc.).
- A new design with best practices, including dependency inversion, testability, modularity, maintainability, and extensibility.
- Jwt based extensible authentication, authorization, identity management
- Improved logging mechanism

## Development Environment

Development environment for linux or mac

```bash

# install python package manager uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# add $HOME/.local/bin to your PATH, either restart your shell or run
export PATH=$HOME/.local/bin:$PATH

# If there is no git, install it from https://git-scm.com/downloads
# Linux command to install git
# apt update; apt install git -y

# Mac command to install git
# brew install git

# clone project from github
git clone https://github.com/EBI-Metabolights/mtbls-ws3.git

cd mtbls-ws3

# install python with uv
uv python install 3.13

# install python dependencies
uv sync --extra ws3

# install pre-commit to check repository integrity and format checking
uv run pre-commit
#########################################################################################
# 🔍 Validate pyproject.toml...................................(no files to check)Skipped
# 🔒 Security · Detect hardcoded secrets...........................................Passed
# 🟢  Markdown · Format markdown...................................................Passed
# 🟢  Check large files............................................................Passed
# 🟢  Check toml files.........................................(no files to check)Skipped
# 🟢  Check json files.........................................(no files to check)Skipped
# 🟢  Format json files........................................(no files to check)Skipped
# 🟢  Check yaml files.............................................................Passed
# 🟢 Check end of file character...................................................Passed
# 🟢  Check training whitespaces...................................................Passed
# ✅ Check training whitespaces................................(no files to check)Skipped
# 🔍 Detect missing __init__.py files..............................................Passed
# 🔍 Check Format with Ruff....................................(no files to check)Skipped
# 🐍 python · Format with Ruff.................................(no files to check)Skipped
# 🪵 architecture and package structure check (lint-imports).......................Passed
#########################################################################################

uv run pytest

# Run and test webservice server on local
# test curator metabolights-help@ebi.ac.uk with password test.123
# test user metabolights-dev@ebi.ac.uk with password test.123
# test studies MTBLS800001 MTBLS800002 MTBLS800003
uv run python tests/run/main_local.py

# open your IDE (vscode, pycharm, etc.) and set python interpreter as .venv/bin/python

```

## Design & Development

You can find design details [Design Section](design/architecture.md)

Before committing and pushing your updates:

- Add/update unit tests.
- Run following commands and fix all issues and

```bash

uv run lint-imports

uv run pre-commit

uv run pytest

```
