# Data Modeling with PostgreSQL

This repository contains all the files for the first project of the Data Engineer Nanodegree Program by Udacity.

"A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app."

## Installation
Use the package manager [pip](https://pip.pypa.io/en/stable/) to install the dependecies (it is recommended to create a virtual environment before doing that).

```bash
pip install -r requirements.txt
```

In addition, we are assuming that a PostgreSQL database is already installed and properly configured, as indicated within the course. In addition, it is important to note that we need to have all the files we are goint to process under a **data** directory inside the project (the same way we have them inside the project workspace). 

## Usage

First we need to create all the tables:
```
python create_tables.py
```

If everything goes fine, this script should end without any warning or errors. Then, we are ready to launch the ETL.
```
python etl.py
```

We should see something like this:

```
71 files found in data/song_data
1/71 files processed.
2/71 files processed.
3/71 files processed.
4/71 files processed.
5/71 files processed.
```

When all the files are processed the script will end and we should be able to query our database.

## The files
**sql_queries.py**
Contains all the queries needed to perform the basic operations in the database (tables creation, inserts, and queries)

**create_tables.py**
Script that allows us to create the tables in the database from scratch. If the tables already exist, they are removed.

**etl.py**
Script that performs the proper ETL process. It process two types of files: songs and log files.

**etl.ipnbb**
Notebook with tests in order to proper understand the ETL process. This is a baseline for the actual ETL (etl.py).

**test.ipynb**
Basic tests to ensure that nothing obvious is missing.


