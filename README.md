# CRISalid directory bridge

## Overview

### Main use case

CRISalid directory bridge is a backend component of CRISalid modular CRIS (current research information system).

It is intended to watch people and organization data from various institutional sources (e.g. LDAP directory, custom HR
databases,
Excel files, laboratory apps, etc.)
and to convert it into a common format that can be fed into CRISalid institutional knowledge graph and other information
system components.

The bridge is designed to be modular and extensible, so that it can be easily adapted to local data sources and formats,
especially in the case when :

- the institutional LDAP directory is not up-to-date but other reliable sources are available, like the National
  Research Structure Registry (RNSR)
- the institutional LDAP directory is not fully [Supann](https://services.renater.fr/documentation/supann/index)
  compliant or conforms to an old version of the Supann schema
- part of the laboratories or institution divisions have their own directories from which data can be extracted
- some kind of structures of singular type are not referenced in the LDAP directory but are described by other sources

### Features

- Watch one or several sources of data (LDAP, RNSR, HR databases, Excel files, REST APIs, etc.) and monitor changes (
  added/removed/updated entries)
- Convert data into a common data model
  approaching [CERIF-2 (Common European Research Information Format)](https://github.com/EuroCRIS/CERIF-Core) to allow
  integration into CRISalid knowledge graph
- Broadcast changes to other CRISalid components (e.g. CRISalid knowledge graph, etc.) through a REST API or a message
  broker (RabbitMQ)

### Usage example

The Xyz research institution wants to extract people and structures information from various sources :

- for the ABC laboratory, the data is available from a laboratory application REST API
- in common cases, the data is available from the institutional LDAP directory, but is overlapping and partially
  conflicting with the data from the ABC laboratory
- for a very particular DEF Research group, the data is only available from an Excel file that can be uploaded by the
  research department

In this particular case, the CRISalid directory bridge can be configured to watch the ABC laboratory application REST
API, the institutional LDAP directory and the Excel file upload endpoint.

The bridge will convert the data from these sources into a common format and broadcast it to the CRISalid knowledge
graph, wich will apply rules defined at the institution level to merge and deduplicate the data.

## Installation

CRISalid directory bridge is a set of configurable pipelines implemented as Airflow DAGs.

### Prerequisites

- You can browse [Apache Airflow documentation](https://airflow.apache.org/docs/apache-airflow/stable/start/local.html)
  but don't install AirFlow as it will be automatically downloded by the pip installation process !
- Install the Python dependencies listed in the `requirements.txt` file.
```shell
pip install -r requirements.txt
```
- Configure Airflow to use a database backend supporting concurrent access (e.g. PostgreSQL) and make the "dags_folder"
  point to the directory of the CRISalid directory bridge repository.(i.e. the directory containing the
  present `README.md` file).

### Configuration

- Create the dotenv files matching the specific environment you want to run the bridge in : e.g. .end.dev for
  development, .env.test for test, etc., by copying the `.env.example` file.

### Running the bridge

- Run the tests to ensure that the bridge is correctly installed and configured.

```shell
AIRFLOW_HOME=~/airflow pytest
```

Provide the path to your Airflow home directory if it is different from the default one.

- Start the Airflow webserver and scheduler.

```shell
airflow standalone
```

Note that the `standalone` command is a custom command that starts the webserver and scheduler in the same process. It
is useful for development and testing purposes only.