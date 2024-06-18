from sqlalchemy import Table, create_engine, MetaData, desc, func, select

mysql_url = "mysql+pymysql://user:password@localhost:3306/exampledb"
metadata = MetaData()
engine = create_engine(mysql_url)

census = Table('census', metadata, autoload_with=engine)

print(f"Table representation: {repr(census)}")

print(f"create table census(\n")
for column in census.columns:
    print(f"{column.name} {column.type},")
print(f")\n")

print(f"create table state_fact(\n")
for column in census.columns:
    print(f"{column.name} {column.type},")
print(f")\n")

connection = engine.connect()

# Build query to return state names by population difference from 2008 to 2000: stmt
stmt = select(
    census.columns.state,
    func.sum(census.columns.pop2008-census.columns.pop2000).label("pop_change")
)
stmt_grouped = stmt.group_by(census.columns.state)
stmt_ordered = stmt_grouped.order_by(desc("pop_change"))
stmt_limit = stmt_ordered.limit(5)

results = connection.execute(stmt_limit).fetchall()
for result in results:
    print(f"{result.state}:{result.pop_change}")