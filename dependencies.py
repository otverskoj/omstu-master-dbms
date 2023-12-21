import os

import psycopg


__all__ = ['get_connection']


async def get_connection() -> psycopg.Connection:
    host = os.getenv('POSTGRES_HOST', '127.0.0.1')
    port = os.getenv('POSTGRES_PORT', '5432')
    user = os.getenv('POSTGRES_USER', 'postgres')
    password = os.getenv('POSTGRES_PASSWORD', 'postgres')
    db_name = os.getenv('POSTGRES_DATABASE', 'postgres')
    connection_string = f"host={host} port={port} user={user} password={password} dbname={db_name}"

    with psycopg.connect(connection_string) as connection:
        yield connection
