import os

import psycopg2
from dotenv import load_dotenv
from psycopg2 import extras, sql

from src.utils.logger import logger


class TimescaleDBOps:
    def __init__(self):
        load_dotenv()
        self.__conn = psycopg2.connect(
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
        )

    def create_table(self, table_name: str, columns: dict, primary_key=None):
        """Create a table in the TimescaleDB database"""
        try:
            with self.__conn.cursor() as cursor:
                # Construct SQL String Composition for Columns
                columns = [
                    sql.SQL("{} {}").format(sql.Identifier(column), sql.SQL(data_type))
                    for column, data_type in columns.items()
                ]

                # Construct SQL String Composition for Primary Key
                primary_key = (
                    sql.SQL(", PRIMARY KEY ({})").format(
                        sql.SQL(", ").join(map(sql.Identifier, primary_key))
                    )
                    if primary_key
                    else sql.SQL("")
                )

                # Construct SQL String Composition for Table Creation
                query = sql.SQL(
                    "CREATE TABLE IF NOT EXISTS {table_name} ({columns} {primary_key});"
                ).format(
                    table_name=sql.Identifier(table_name),
                    columns=sql.SQL(", ").join(columns),
                    primary_key=primary_key,
                )
                cursor.execute(query)
                self.__conn.commit()
                logger.info(f"Table {table_name} created successfully.")
        except (psycopg2.DatabaseError, Exception) as error:
            logger.warning(error)
            self.__conn.rollback()

    def create_hypertable(self, table_name: str, time_column: str):
        """Create a hyper table in the TimescaleDB database"""
        try:
            with self.__conn.cursor() as cursor:
                # Construct SQL String Composition for Hypertable Creation
                query = sql.SQL(
                    "SELECT create_hypertable({table_name}, by_range({time_column}));"
                ).format(
                    table_name=sql.Literal(table_name),
                    time_column=sql.Literal(time_column),
                )
                cursor.execute(query)
                self.__conn.commit()
                logger.info(f"Hypertable {table_name} created successfully.")
        except (psycopg2.DatabaseError, Exception) as error:
            logger.warning(error)
            self.__conn.rollback()

    def execute_query(self, query: sql.SQL):
        try:
            with self.__conn.cursor() as cursor:
                cursor.execute(query)
                self.__conn.commit()
                logger.info(f"Query Execute Successfuly")
        except (psycopg2.DatabaseError, Exception) as error:
            logger.warning(error)
            self.__conn.rollback()

    def executemany_query(self, query: sql.SQL, data: list):
        try:
            with self.__conn.cursor() as cursor:
                cursor.executemany(query, data)
                self.__conn.commit()
                logger.info(f"Query Execute Successfuly")
        except (psycopg2.DatabaseError, Exception) as error:
            logger.warning(error)
            self.__conn.rollback()

    def insert_data(
        self, table_name: str, columns: list, values: list, conflict_columns: list
    ):
        """Insert data into the TimescaleDB database"""
        try:
            with self.__conn.cursor() as cursor:
                query = sql.SQL(
                    """
                    INSERT INTO {table_name} ({columns}) 
                    VALUES ({values})
                    ON CONFLICT ({conflict_columns})
                    DO UPDATE SET
                        {updates}
                    """
                ).format(
                    table_name=sql.Identifier(table_name),
                    columns=sql.SQL(", ").join(map(sql.Identifier, columns)),
                    values=sql.SQL(", ").join(sql.Literal(value) for value in values),
                    conflict_columns=sql.SQL(",").join(
                        [
                            sql.SQL("{column}").format(column=sql.Identifier(column))
                            for column in conflict_columns
                        ]
                    ),
                    updates=sql.SQL(",").join(
                        [
                            sql.SQL("{column} = EXCLUDED.{column}").format(
                                column=sql.Identifier(column)
                            )
                            for column in columns
                            if column not in conflict_columns
                        ]
                    ),
                )
                cursor.execute(query)
                self.__conn.commit()
                logger.info(f"Data inserted into {table_name} successfully.")
        except (psycopg2.DatabaseError, Exception) as error:
            logger.warning(error)
            self.__conn.rollback()

    def batch_insert_data(
        self,
        table: str,
        schema: str,
        columns: tuple,
        data: list,
        conflict_columns: list,
    ):
        """Insert data into the TimescaleDB database"""
        try:
            with self.__conn.cursor() as cursor:
                # Set schema
                query = sql.SQL("SET search_path TO {schema}").format(
                    schema=sql.Identifier(schema)
                )
                cursor.execute(query)

                # Insert Data
                query = sql.SQL(
                    """
                    INSERT INTO {table} ({columns}) 
                    VALUES ({values})
                    ON CONFLICT ({conflict_columns})
                    DO UPDATE SET
                        {updates}
                    """
                ).format(
                    table=sql.Identifier(table),
                    columns=sql.SQL(", ").join(map(sql.Identifier, columns)),
                    values=sql.SQL(", ").join(sql.Placeholder() for _ in columns),
                    conflict_columns=sql.SQL(",").join(
                        [
                            sql.SQL("{column}").format(column=sql.Identifier(column))
                            for column in conflict_columns
                        ]
                    ),
                    updates=sql.SQL(",").join(
                        [
                            sql.SQL("{column} = EXCLUDED.{column}").format(
                                column=sql.Identifier(column)
                            )
                            for column in columns
                            if column not in conflict_columns
                        ]
                    ),
                )
                cursor.executemany(query, data)
                self.__conn.commit()
                logger.info(f"Data inserted into {table} successfully.")
        except (psycopg2.DatabaseError, Exception) as error:
            logger.warning(error)
            self.__conn.rollback()

    def read_data(self, table_name):
        """Read data from the TimescaleDB database"""
        try:
            with self.__conn.cursor() as cursor:
                select_query = sql.SQL(
                    """
                    SELECT * FROM {table_name}
                """
                ).format(
                    table_name=sql.Identifier(
                        table_name.split(".")[0], table_name.split(".")[1]
                    ),
                )
                cursor.execute(select_query)
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                logger.info(f"Data read from {table_name} successfully.")
                return (columns, rows)
        except (psycopg2.DatabaseError, Exception) as error:
            logger.warning(error)
            self.__conn.rollback()

    def close_connection(self):
        """Close the TimescaleDB database connection"""
        if self.__conn:
            self.__conn.close()
            logger.info("TimescaleDB connection closed.")
        else:
            logger.info("No connection to close.")
