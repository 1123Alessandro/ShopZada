CREATE DATABASE airflow_db;
CREATE DATABASE shopzada;
CREATE USER ShopZada WITH PASSWORD 'pass' SUPERUSER;
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO ShopZada;
GRANT ALL ON SCHEMA public TO ShopZada;
