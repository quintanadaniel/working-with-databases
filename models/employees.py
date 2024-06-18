
from sqlalchemy import Boolean, DECIMAL, Integer, String, Table, create_engine, MetaData, Column, ForeignKey

metadata = MetaData()

employees = Table(
    "employees",
    metadata,
    Column("id", Integer()),
    Column("name", String(255)),
    Column("salary", DECIMAL),
    Column("active", Boolean())
)

metadata.create_all(engine)
