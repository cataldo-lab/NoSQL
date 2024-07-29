from neo4j import GraphDatabase

class Neo4jConnection:

    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def query(self, query, parameters=None, db=None):
        with self._driver.session(database=db) as session:
            result = session.run(query, parameters)
            return [record for record in result]

# Conectar a Neo4j
conn = Neo4jConnection(uri="bolt://localhost:7687", user="neo4j", password="toroE2024")

# Verificar la conexión
try:
    conn.query("MATCH (n) RETURN n LIMIT 1")
    print("Connected to Neo4j successfully")
except Exception as e:
    print(f"Connection error: {e}")

# Consulta para encontrar el vino más caro por cada país
query_max_price_per_country = """
MATCH (n:points)-[:RELATED_TO]->(c:vino)
WITH c.country AS country, c.designation AS designation, n.price AS price, n.points AS points
ORDER BY country, price DESC
WITH country, head(collect({designation: designation, price: price, points: points})) AS top_wine
RETURN country, top_wine.designation AS designation, top_wine.price AS price, top_wine.points AS points
ORDER BY top_wine.price DESC
"""

try:
    result_max_price_per_country = conn.query(query_max_price_per_country)
    if result_max_price_per_country:
        for record in result_max_price_per_country:
            print(f"Country: {record['country']}, Designation: {record['designation']}, Price: {record['price']}, Points: {record['points']}")
    else:
        print("No results found")
except Exception as e:
    print(f"Query error: {e}")

# Cerrar la conexión
conn.close()
