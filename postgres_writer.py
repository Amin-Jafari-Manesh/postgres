import logging
from os import environ
from datetime import datetime
import psycopg2
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime


db_config = {
    'PASS': environ.get('PASS', ''),
    'DOMAIN': environ.get('DOMAIN', ''),
    'HASH_SIZE': environ.get('HASH_SIZE', ''),
    'RECORDS': environ.get('RECORDS', ''),
}
metadata = MetaData()

hashes = Table('hashes', metadata,
               Column('id', Integer, primary_key=True),
               Column('hash', String(64), unique=True),
               Column('created_at', DateTime, default=datetime.now)
               )


def generate_random_hash(numb: int = 1) -> str:
    import random
    import string
    import hashlib
    if numb == 1:
        return hashlib.sha256(''.join(random.choices(string.ascii_letters + string.digits, k=64)).encode()).hexdigest()
    else:
        return ''.join(
            [hashlib.sha256(''.join(random.choices(string.ascii_letters + string.digits, k=64)).encode()).hexdigest()
             for _ in range(numb)])


def test_postgres_connection():
    try:
        conn = psycopg2.connect(
            dbname='db',
            user='postgres',
            password=db_config['PASS'],
            host=db_config['DOMAIN'],
            port='5432'
        )
        logging.info("PostgreSQL connection successful")
        conn.close()
        return True
    except Exception as e:
        logging.error(f"PostgreSQL connection failed: {e}")
        return False


def postgres_write_hash(size: int = 100) -> bool:
    if test_postgres_connection():
        postgres_uri = f"postgresql+psycopg2://postgres:{db_config['PASS']}@{db_config['DOMAIN']}:5432/db"
        engine = create_engine(postgres_uri)
        connection = engine.connect()
        metadata.create_all(engine)
        for _ in range(size):
            connection.execute(hashes.insert().values(hash=generate_random_hash(db_config['HASH_SIZE'])))
        connection.close()
        return True
    return False
