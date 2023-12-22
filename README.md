# Project Name

MongoDB log analyser

## Table of Contents

- [Project Name](#project-name)
  - [Table of Contents](#table-of-contents)
  - [Introduction](#introduction)
  - [Features](#features)
  - [Getting Started](#getting-started)
    - [Prerequisites](#prerequisites)
    - [Installation](#installation)

## Introduction

Explain what your project is about and why it's useful. Provide some context and background information.

## Features

List the key features of your project. You can use bullet points for this:

- Feature 1
- Feature 2
- Feature 3

## Getting Started

Provide instructions on how to get started with your project.

### Prerequisites
The list of variables required by one or more scripts that are expected to be in the environment.
Add by either configuration in IDE or "export" command.

- dbuser: currently used for Atlas connection string only
- dbpass: currently used for Atlas connection string only

### Installation

Tu run the binaries you may allow executing them via the settings privacy & security.
```bash
$ git clone git@github.com:StasBas/MDB_TS_Tools.git
$ cd MDB_TS_Tools
$ chmod +x ./MDB_TS_Tools/binaries/mac_ARM_build/connections_analyzer
$ ./MDB_TS_Tools/bonaries/mac_ARM_build/connections_analyzer
```

To run the code:

May require installation of virtualenv:

```bash
pip3 install virtualenv
```


```bash
$ git clone git@github.com:StasBas/MDB_TS_Tools.git
$ cd MDB_TS_Tools
$ virtualenv env
$ source env/bin/activate
$ pip3 install -r requirements.txt
$ python3 -m ./MDB_TS_Tools/queries_analyzer/queries_analyzer.py
```

