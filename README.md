# MetaboLights v3

You can find technical documentation on (this website)[https://ebi-metabolights.github.io/mtbls-ws3/]

## Development Environment

Development environment for linux or mac

```bash

# install python package manager uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# add $HOME/.local/bin to your PATH
export PATH=$HOME/.local/bin:$PATH

# RESTART your terminal or OPEN NEW one

# install git from https://git-scm.com/downloads
# Linux command
apt update; apt install git -y

# Mac command
# brew install git
# clone project from github
git clone https://github.com/EBI-Metabolights/mtbls-ws3

cd mtbls-ws3

# install python using uv tool.
uv python install 3.13

# install python dependencies
uv sync

# run lint-imports verify architecture
uv run lint-imports
# =============
# Import Linter
# =============

# ---------
# Contracts
# ---------

# Analyzed 448 files, 1603 dependencies.
# --------------------------------------

# Architecture Layer Dependencies KEPT
# Restricted Async Task Imports KEPT
# Restricted Dependency Injection KEPT
# Independent Implementations in Infrastructure Layer KEPT
# Independent Rest API Groups in Presentation Layer KEPT
# External Framework Independence (Infrastructure) KEPT
# External Framework Independence (Presentation) KEPT
# No Business Logic In Infrastructure Layer KEPT
# No Business Logic In Utils KEPT


# install pre-commit to check repository integrity and format checking
uv run pre-commit

# install pre-commit to check repository integrity and format checking
uv run pytest

# Run and test webservice server on local
# test curator metabolights-help@ebi.ac.uk with password test.123
# test user metabolights-dev@ebi.ac.uk with password test.123
# test studies MTBLS800001 MTBLS800002 MTBLS800003
uv run python tests/run/main_local.py

# open your IDE (vscode, pycharm, etc.) and set python interpreter as .venv/bin/python

```

## Update repository

Run following tasks before committing and pushing your updates, and fix all issues.

```bash

uv run lint-imports

uv run pre-commit

uv run pytest

```
