# Upgrade CRISalid Directory Bridge from Airflow 2.9 to Airflow 3.0

## Reference

- Airflow 3 upgrade guide: https://airflow.apache.org/docs/apache-airflow/stable/installation/upgrading_to_airflow3.html

## 1. Install Airflow 3.0.0

`requirements.txt` has been updated with new Airflow versions, so you can install Airflow 3.0.0 with the following
command:

```bash
pip install -r requirements.txt
```


### 2. Upgrade `airflow.cfg`
Use the following commands to upgrade your configuration:

```bash
# dry run
airflow config update

# apply changes
airflow config update --fix
```

If you want to keep the same authentication manager (recommended):

```ini
[core]
auth_manager = airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager
```

Append the following lines to the end of `airflow.cfg`:

```ini
[api_auth]
jwt_secret = here-your-key
```

Compute the key with

```bash
 openssl rand -base64 16
```

### 3. Migrate the Database Schema

```bash 
airflow db migrate
```

You may see warnings about `airflow.cfg` keys being moved:

```
The secret_key option in [webserver] has been moved to the secret_key option in [api] - the old setting has been used, but please update your config.
The web_server_port option in [webserver] has been moved to the port option in [api] - the old setting has been used, but please update your config.
The workers option in [webserver] has been moved to the workers option in [api] - the old setting has been used, but please update your config.
The web_server_host option in [webserver] has been moved to the host option in [api] - the old setting has been used, but please update your config.
The access_logfile option in [webserver] has been moved to the access_logfile option in [api] - the old setting has been used, but please update your config.
The web_server_ssl_cert option in [webserver] has been moved to the ssl_cert option in [api] - the old setting has been used, but please update your config.
The web_server_ssl_key option in [webserver] has been moved to the ssl_key option in [api] - the old setting has been used, but please update your config.
```

Update your `airflow.cfg` accordingly to avoid these warnings.


### 5. Start Airflow 3

```bash
airflow standalone
```