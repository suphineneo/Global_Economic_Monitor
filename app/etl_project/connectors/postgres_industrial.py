from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.engine import URL
from sqlalchemy.dialects import postgresql
import pandas as pd
import numpy as np


class PostgreSqlClient:
    """
    A client for querying PostgreSQL database.
    """

    def __init__(
        self,
        server_name: str,
        database_name: str,
        username: str,
        password: str,
        port: int = 5432,
    ):
        self.host_name = server_name
        self.database_name = database_name
        self.username = username
        self.password = password
        self.port = port

        connection_url = URL.create(
            drivername="postgresql+pg8000",
            username=username,
            password=password,
            host=server_name,
            port=port,
            database=database_name,
        )

        self.engine = create_engine(connection_url)

    def select_all(self, table: Table) -> list[dict]:
        """
        Select all rows from the specified table.
        """
        with self.engine.connect() as conn:
            return [dict(row) for row in conn.execute(table.select()).all()]

    def create_table(self, metadata: MetaData) -> None:
        """
        Creates a table defined in the metadata object.
        """
        metadata.create_all(self.engine)

    def drop_table(self, table_name: str) -> None:
        """
        Drops a table if it exists.
        """
        with self.engine.connect() as conn:
            conn.execute(f"DROP TABLE IF EXISTS {table_name};")

    def insert(self, data: list[dict], table: Table, metadata: MetaData) -> None:
        """
        Inserts data into a table.
        """
        metadata.create_all(self.engine)
        insert_statement = postgresql.insert(table).values(data)
        with self.engine.connect() as conn:
            conn.execute(insert_statement)

    def overwrite(self, data: list[dict], table: Table, metadata: MetaData) -> None:
        """
        Drops the table and recreates it with new data.
        """
        self.drop_table(table.name)
        self.insert(data=data, table=table, metadata=metadata)

    def upsert(self, data: list[dict], table: Table, metadata: MetaData) -> None:
    
        """
        Performs an UPSERT (insert or update on conflict), ensuring no duplicate primary key entries are present in the data.
        """
        
        metadata.create_all(self.engine)

        # Convert list of dicts to DataFrame to drop duplicates
        df = pd.DataFrame(data)
        df = df.drop_duplicates(subset=['year', 'country_code'])

        # Convert back to list of dicts
        data = df.to_dict(orient="records")

        key_columns = [pk_column.name for pk_column in table.primary_key.columns.values()]

        insert_statement = postgresql.insert(table).values(data)

        # Perform conflict resolution: update non-key columns if conflict occurs
        upsert_statement = insert_statement.on_conflict_do_update(
            index_elements=key_columns,
            set_={
                col.key: col for col in insert_statement.excluded if col.key not in key_columns
            }
        )
        with self.engine.connect() as conn:
            conn.execute(upsert_statement)