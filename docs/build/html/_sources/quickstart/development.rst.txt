########################
Development installation
########################
This guide will walk you through setting up CRISalid Directory Bridge project on a fresh Ubuntu 22.04 installation.


1. Set the environment
-----------------------
At the root of the project, set the .env matching the specific environment you want to run the bridge in (env.dev, env.test...) by copying the .env.example file available.

We will update the .env settings to reflect the parameters used to

.. seealso::
     :doc:`../environment`
      Detail of the parameters in the .env

2. Install PostgreSQL
-----------------------

.. tab-set::

    .. tab-item:: On platform

        Follow the steps outlined on `PostgreSQL's official documentation <https://www.postgresql.org/download/linux/ubuntu/>`_:

        .. code-block:: bash

           # Add PostgreSQL repository
           sudo sh -c 'echo "deb https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
           wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
           sudo apt-get update

           # Install PostgreSQL
           sudo apt-get -y install postgresql-16

           # Create PostgreSQL database and user
           sudo -u postgres psql

    .. tab-item:: On docker

        With Docker installed, use the following commands:

        .. code-block:: bash

            docker pull postgres
            docker run --name svp_postgres -e POSTGRES_PASSWORD=svph -e POSTGRES_USER=svph -p 5432:5432 -d postgres

In the PostgreSQL shell use the following command to create the database who will be used by the application.
In the example below, a database set with the values found in .env.example

.. code-block:: sql

    CREATE DATABASE crisalid_directory_bridge;
    CREATE USER cdb_user WITH PASSWORD 'cdb_pwd';
    GRANT ALL PRIVILEGES ON DATABASE crisalid_directory_bridge TO cdb_user;
    -- PostgreSQL 15 requires additional privileges:
    GRANT ALL ON SCHEMA public TO cdb_user;


3. Install Apache Airflow
----------------------------

.. important::
    Apache Airflow need to be installed outside of the application folder.

- Install Airflow following the steps you can find in `Airflow Quick Start <https://airflow.apache.org/docs/apache-airflow/stable/start.html)>`_.

- Configure Airflow to use a PostreSQL database, following `Airflow setting up a PostgreSQL database <https://airflow.apache.org/docs/apache-airflow/stable/howto/set-up-database.html#setting-up-a-postgresql-database>`_

.. note ::
    If you followed "Install PostgreSQL" section, you already created the database needed to use airflow.



4. Install RabbitMQ
----------------------

.. tab-set::

    .. tab-item:: On platform

        To install RabbitMQ on your system, please follow the steps outlined in the `official RabbitMQ documentation <https://www.rabbitmq.com/docs/install-debian>`_.
        Once RabbitMQ is installed, enable the management interface by executing the following commands:

        .. code-block:: bash

            # Enable the management interface
            sudo rabbitmq-plugins enable rabbitmq_management

            # Restart RabbitMQ
            sudo systemctl restart rabbitmq-server

    .. tab-item:: On docker

        With Docker installed, use the following commands:

        .. code-block:: bash

            docker pull rabbitmq:3.13-management

            docker run -d --hostname my-rabbit --name rabbitmq --publish=15672:15672 --publish=5672:5672 rabbitmq:3.13-management

After completing these steps, access the management interface through your web browser by navigating to `localhost:15672`. You can log in using the default credentials: ``guest:guest``.

5. Install Redis
----------------------


.. tab-set::

    .. tab-item:: On platform

        Refer to the `Redis documentation <https://redis.io/docs/latest/operate/oss_and_stack/install/install-redis/install-redis-on-linux/>`_ for installation instructions:

        .. code-block:: bash

           curl -fsSL https://packages.redis.io/gpg | sudo gpg --dearmor -o /usr/share/keyrings/redis-archive-keyring.gpg

           echo "deb [signed-by=/usr/share/keyrings/redis-archive-keyring.gpg] https://packages.redis.io/deb $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/redis.list

           sudo apt-get update
           sudo apt-get install redis

    .. tab-item:: On docker

        With Docker installed, use the following commands:

        .. code-block:: bash

            docker pull redis/redis-stack:latest
            docker run -d --name redis -p 6379:6379 -p 8001:8001 redis/redis-stack:latest



6. Set the project
----------------------
At the root of the project,

- Install the requirements:

.. code-block:: bash

    pip install -r requirements.txt

- Run the tests to ensure that the bridge is correctly installed and configured:

.. code-block:: bash

    AIRFLOW_HOME=~/airflow pytest

- Start the airflow webserver an scheduler

.. code-block:: bash

    airflow standalone

.. note::
    Note that the `standalone` command is a custom command that starts the webserver and scheduler in the same process. It
    is useful for development and testing purposes only.