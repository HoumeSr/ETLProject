clickhouse_insert_query = """
INSERT INTO purchases (gender, price, quantity, timestamp)
VALUES (%s, %s, %s, %s)
"""

clickhouse_create_query = """
CREATE TABLE purchases (
    id UUID DEFAULT generateUUIDv4(),
    gender String,
    price Float64,
    quantity Float64,
    timestamp DateTime 
) ENGINE = MergeTree()
ORDER BY (id, timestamp);
"""

t = """
DROP TABLE IF EXISTS purchases
"""
