############################
Setting the app environment
############################

To configure your application, you'll need to set specific environment variables in a `.env` file. This file is essential for connecting to various services such as LDAP, RabbitMQ, Redis, and your database. Below, you'll find each category of settings, along with explanations for each variable.

LDAP Configuration
------------------
These settings define how your application connects to and interacts with the LDAP server.

**Connection Settings:**
  - **LDAP_HOST**: URI for your LDAP server (e.g., `ldap://ldap-host:ldap-port`)
  - **LDAP_BIND_DN**: The Distinguished Name used for authentication.
  - **LDAP_BIND_PASSWORD**: Password for the LDAP bind user.

**Branch and Filter Configuration:**
  - **LDAP_STRUCTURES_BRANCH**: Base path in LDAP for querying structures.
  - **LDAP_PEOPLE_BRANCH**: Base path in LDAP for querying people.
  - **LDAP_STRUCTURES_FILTER**: Filter string for retrieving specific structures.
  - **LDAP_PEOPLE_FILTERS**: Filter criteria for identifying individuals with specific affiliations.
  - **LDAP_PEOPLE_FILTER_PATTERN**: Pattern for filtering members, using `%s` as a placeholder for dynamic content.

**Task Management:**
  - **LDAP_STRUCTURE_IDENTIFIER_TASK**: Task for processing structure identifiers.
  - **LDAP_STRUCTURE_NAME_TASK**: Task for processing structure names.
  - **LDAP_STRUCTURE_ACRONYM_TASK**: Task for processing structure acronyms.
  - **LDAP_STRUCTURE_DESCRIPTION_TASK**: Task for processing structure descriptions.
  - **LDAP_STRUCTURE_ADDRESS_TASK**: Task for processing structure addresses.
  - **LDAP_PERSON_NAME_TASK**: Task for processing people names.
  - **LDAP_PERSON_IDENTIFIER_TASK**: Task for processing people identifiers.
  - **LDAP_PERSON_MEMBERSHIP_TASK**: Task for managing memberships.

**Additional Setting:**
  - **LDAP_DEFAULT_LANGUAGE**: Default language for LDAP data, e.g., `fr` for French.

Spreadsheet Paths
-----------------
The application uses these file paths to locate CSV files containing people, structure, and identifier data.

  - **SPREADSHEET_PEOPLE_PATH**: Path to the CSV file with people data.
  - **SPREADSHEET_STRUCTURES_PATH**: Path to the CSV file with structure data.
  - **SPREADSHEET_IDENTIFIERS_PATH**: Path to the CSV file with identifiers data.

RabbitMQ Configuration
----------------------
RabbitMQ handles message queuing for the application. Use these settings to connect:

  - **RABBITMQ_CONN_ID**: Connection ID for RabbitMQ within Airflow.
  - **RABBITMQ_HOST**: RabbitMQ server hostname (often `localhost`).
  - **RABBITMQ_PORT**: RabbitMQ server port (default: `5672`).
  - **RABBITMQ_USER**: Username for RabbitMQ.
  - **RABBITMQ_PASSWORD**: Password for RabbitMQ user.

Redis Configuration
-------------------
Redis is used for caching data. Configure the Redis connection here:

  - **REDIS_CONN_ID**: Connection ID for Redis within Airflow.
  - **REDIS_HOST**: Redis server hostname, often `localhost`.
  - **REDIS_PORT**: Redis port, typically `6379`.
  - **REDIS_PASSWORD**: Password for Redis access.

Database Configuration
----------------------
These variables configure your database connection for storing and retrieving application data.

  - **DB_NAME**: Database name.
  - **DB_USER**: Username for database access.
  - **DB_PASSWORD**: Password for the database user.
  - **DB_HOST**: Database server hostname (usually `localhost`).

