CREATE DATABASE airflow_db;
CREATE USER ShopZada WITH PASSWORD 'pass';
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO ShopZada;
GRANT ALL ON SCHEMA public TO ShopZada;
