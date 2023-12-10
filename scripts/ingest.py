import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, BigInteger, TIMESTAMP, DATE, ForeignKey
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
import psycopg2

#create database first before running

#Adjust user, password, etc. according to own implementation
engine = create_engine('postgresql://postgres:kyle1018@localhost:5432/ShopZada')

Base = declarative_base()

class campaign(Base):
    __tablename__ = 'campaign'

    CAMPAIGN_ID = Column(String, primary_key=True)
    CAMPAIGN_NAME = Column(String)
    CAMPAIGN_DESCRIPTION = Column(String)
    DISCOUNT_PERCENTAGE = Column(String)

class customer(Base):
    __tablename__ = 'customer'
    CUSTOMER_ID = Column(String, primary_key=True)
    CUSTOMER_NAME = Column(String)
    CUSTOMER_CREDIT_CARD_NUM = Column(BigInteger)
    CUSTOMER_CREDIT_CARD_BANK = Column(String)
    CUSTOMER_REGISTRATION_DATE = Column(TIMESTAMP(timezone=False))
    CUSTOMER_STREET = Column(String)
    CUSTOMER_STATE = Column(String)
    CUSTOMER_CITY = Column(String)
    CUSTOMER_COUNTRY = Column(String)
    CUSTOMER_BIRTH_DATE = Column(TIMESTAMP(timezone=False))
    CUSTOMER_GENDER = Column(String)
    CUSTOMER_DEVICE_ADDRESS = Column(String)
    CUSTOMER_TYPE = Column(String)
    CUSTOMER_JOB_TITLE = Column(String)
    CUSTOMER_JOB_LEVEL = Column(String)

class date(Base):
    __tablename__ = 'date'
    DATE_ID = Column(DATE, primary_key=True)
    DAY_OF_WEEK = Column(Integer)
    MONTH = Column(Integer)
    YEAR = Column(Integer)
    
class merchant(Base):
    __tablename__ = 'merchant'
    MERCHANT_ID = Column(String, primary_key=True)
    MERCHANT_CREATION_DATE = Column(TIMESTAMP(timezone=False))
    MERCHANT_NAME = Column(String)
    MERCHANT_STREET = Column(String)
    MERCHANT_STATE = Column(String)
    MERCHANT_CITY = Column(String)
    MERCHANT_COUNTRY = Column(String)
    MERCHANT_CONTACT_NUMBER = Column(String)
    
class order(Base):
    __tablename__ = 'order'
    ORDER_ID = Column(String, primary_key=True)
    ORDER_ESTIMATED_ARRIVAL = Column(Integer)
    ORDER_QUANTITY = Column(Integer)
    
class product(Base):
    __tablename__ = 'product'
    PRODUCT_ID = Column(String, primary_key=True)
    PRODUCT_NAME = Column(String)
    PRODUCT_TYPE = Column(String)
    PRODUCT_PRICE = Column(DOUBLE_PRECISION)

class staff(Base):
    __tablename__ = 'staff'
    STAFF_ID = Column(String, primary_key=True)
    STAFF_NAME = Column(String)
    STAFF_JOB_LEVEL = Column(String)
    STAFF_STREET = Column(String)
    STAFF_STATE = Column(String)
    STAFF_CITY = Column(String)
    STAFF_COUNTRY = Column(String)
    STAFF_CONTACT_NUMBER = Column(String)
    STAFF_CREATION_DATE = Column(TIMESTAMP(timezone=False))

class campaign_transaction(Base):
    __tablename__ = 'campaign transaction'
    CAMPAIGN_TRANSACTION_FACT_ID = Column(BigInteger, primary_key=True)
    CAMPAIGN_ID = Column(String, ForeignKey('campaign.CAMPAIGN_ID'))
    ORDER_ID = Column(String, ForeignKey('order.ORDER_ID'))
    DATE_ID = Column(DATE, ForeignKey('date.DATE_ID'))

class merchant_transaction(Base):
    __tablename__ = 'merchant performance'
    MERCHANT_PERFORMANCE_FACT_ID = Column(BigInteger, primary_key=True)
    MERCHANT_ID = Column(String, ForeignKey('merchant.MERCHANT_ID'))
    ORDER_ID = Column(String, ForeignKey('order.ORDER_ID'))
    STAFF_ID = Column(String, ForeignKey('staff.STAFF_ID'))
    DATE_ID = Column(DATE, ForeignKey('date.DATE_ID'))

class sales(Base):
    __tablename__ = 'sales'
    SALES_FACT_ID = Column(BigInteger, primary_key=True)
    ORDER_ID = Column(String, ForeignKey('order.ORDER_ID'))
    CUSTOMER_ID = Column(String, ForeignKey('customer.CUSTOMER_ID'))
    DATE_ID = Column(DATE, ForeignKey('date.DATE_ID'))
    PRODUCT_ID = Column(String, ForeignKey('product.PRODUCT_ID'))
    MERCHANT_ID = Column(String, ForeignKey('merchant.MERCHANT_ID'))
    TOTAL_AMOUNT = Column(DOUBLE_PRECISION)
    DELAY_IN_DAYS = Column(Integer)

# Create the tables
Base.metadata.create_all(engine)


# ingest dimensions
pd.read_parquet('./exports/order_dimension.parquet').to_sql('order', engine, index=False, if_exists='append')
pd.read_parquet('./exports/staff_dimension.parquet').to_sql('staff', engine, index=False, if_exists='append')
pd.read_parquet('./exports/product_dimension.parquet').to_sql('product', engine, index=False, if_exists='append')
pd.read_parquet('./exports/campaign_dimension.parquet').to_sql('campaign', engine, index=False, if_exists='append')
pd.read_parquet('./exports/customer_dimension.parquet').to_sql('customer', engine, index=False, if_exists='append')
pd.read_parquet('./exports/merchant_dimension.parquet').to_sql('merchant', engine, index=False, if_exists='append')
pd.read_parquet('./exports/date_dimension.parquet').to_sql('date', engine, index=False, if_exists='append')

# ingest facts
pd.read_parquet('./exports/sales_fact.parquet').to_sql('sales', engine, index=False, if_exists='append')
pd.read_parquet('./exports/campaign_transaction_fact.parquet').to_sql('campaign transaction', engine, index=False, if_exists='append')
pd.read_parquet('./exports/merchant_performance_fact.parquet').to_sql('merchant performance', engine, index=False, if_exists='append')
