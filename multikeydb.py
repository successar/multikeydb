import json
from typing import Any, Iterable

from sqlalchemy import Column, Integer, MetaData, PrimaryKeyConstraint, String, Table, and_, create_engine

types_to_constructors = {
    int: Integer,
    str: String,
}

class MultiKeyDB:
    def __init__(self, filename: str):
        conn_str = f"sqlite:///{filename}"
        self.engine = create_engine(conn_str, connect_args={"check_same_thread": False})
        self.metadata = MetaData()
        self.metadata.reflect(bind=self.engine)
        self.tables = {}
        for table in self.metadata.tables.values():
            self.tables[table.name] = table

    def create(self, table: str, keys: dict):
        columns = [Column(colname, types_to_constructors[coltype]) for colname, coltype in keys.items()]
        primary_key_columns = [v for v in columns]
        columns.append(Column("value", types_to_constructors[str]))
        primary_key = PrimaryKeyConstraint(*primary_key_columns)
        table = Table(table, self.metadata, *columns, primary_key)
        table.create(self.engine, checkfirst=True)
        self.tables[table.name] = table

    def exist_table(self, table: str) -> bool:
        return table in self.tables

    def where(self, table, keys):
        return and_(*[table.c[key] == value for key, value in keys.items()])

    def put(self, table: str, keys: dict, value: Any):
        value = json.dumps(value)
        assert all([key in keys or key == "value" for key in self.tables[table].columns.keys()])
        table = self.tables[table]
        with self.engine.begin() as connection:
            where_clause = self.where(table, keys)
            q = connection.execute(table.select().where(where_clause)).fetchall()
            if len(q) == 0:
                connection.execute(table.insert().values(value=value, **keys))
            else:
                connection.execute(table.update().values(value=value).where(where_clause))

    def delete(self, table: str, keys: dict):
        assert all(
            [key in keys or key == "value" for key in self.tables[table].columns.keys()]
        ), "Key not complete"
        table = self.tables[table]
        with self.engine.begin() as connection:
            connection.execute(table.delete().where(self.where(table, keys)))

    def filter(self, table: str, keys: dict) -> list:
        table = self.tables[table]
        with self.engine.begin() as connection:
            if len(keys) == 0:
                records = connection.execute(table.select()).fetchall()
            else:
                records = connection.execute(table.select().where(self.where(table, keys))).fetchall()

            remaining_keys = [key for key in table.columns.keys() if key not in keys]
            return [
                {
                    key: getattr(r, key) if key != "value" else json.loads(getattr(r, key))
                    for key in remaining_keys
                }
                for r in records
            ]

    def get(self, table: str, keys: dict) -> Any:
        assert all(
            [key in keys or key == "value" for key in self.tables[table].columns.keys()]
        ), "Key not complete"
        assert "value" not in keys
        table = self.tables[table]

        with self.engine.begin() as connection:
            records = connection.execute(table.select().where(self.where(table, keys))).fetchall()
            if len(records) == 0:
                return None
            else:
                return json.loads(records[0].value)

    def dump(self) -> Iterable[dict]:
        with self.engine.begin() as connection:
            for table in self.tables.values():
                for row in connection.execute(table.select()).fetchall():
                    data = dict(row)
                    data["value"] = json.loads(data["value"])
                    yield {"table": table.name, **data}
