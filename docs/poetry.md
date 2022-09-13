# Using Poetry Package Manager

Poetry is a tool for dependency management and packaging in Python. It allows you to declare the libraries your project depends on and it will manage (install/update) them for you.

## Poetry Setup

## 1. Install

### 1.1 Windows
Must use powershell:
```bash
Invoke-WebRequest -Uri https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py -UseBasicParsing).Content | python -
```

### 1.2 Linux

```bash
curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python -
```

## For Vscode
First need to run ``poetry config virtualenvs.in-project true`` before creating a new venv with ``poetry install``

## 2. Commands (Usage)

### (1) install
```
poetry install
```

### (2) Managing your venv

Check env
```bash
poetry env list 
```
delete an env
```bash
poetry env remove <env-name>
```

### (3) Update a certain dependency

this will update the ``hunter-common`` dependency to the latest version

```shell
poetry update hunter-common
```
