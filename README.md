# ShopZada

CSELEC1C Final Project 

## Setup
> Reminder: This is only for Linux operating systems or at the very least, using the Windows Subsystem for Linux
- Install airflow and its dependencies
- Install postgresql
- Clone this repository and make it the current working directory
- Setup airflow configurations with these commands
```bash
export AIRFLOW_HOME=$(pwd)
export AIRFLOW__CORE__DEFAULT_TIMEZONE=pst
export AIRFLOW__CORE__LOAD_EXAMPLES=False
# export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://shopzada:pass@localhost/airflow_db
airflow db migrate
```
- Create your own admin accounts using the airflow CLI
- Create postgresql database for airflow metadata and the main database with `psql -a -f setupDB.sql`
- run `airflow scheduler -D` to daemonize the scheduler
- run `airflow webserver -D -p 8080` to daemonize the webserver
- access <localhost:8080> and log in with your account

### Developer Testing
```bash
export AIRFLOW_HOME=$(pwd)
export AIRFLOW__CORE__DEFAULT_TIMEZONE=pst
export AIRFLOW__CORE__LOAD_EXAMPLES=False
# export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://shopzada:pass@localhost/airflow_db
airflow db migrate
airflow standalone
```
