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

conn = Neo4jConnection(uri="bolt://localhost:7687", user="neo4j", password="toroE2024")

# Verificar la conexión
try:
    conn.query("MATCH (n) RETURN n LIMIT 1")
    print("Connected to Neo4j successfully")
except Exception as e:
    print(f"Connection error: {e}")

# Actualizar propiedades
query_update = """
MATCH (p:points)
SET p.price = toFloat(p.price),
    p.points = toInteger(p.points)
"""
try:
    conn.query(query_update)
    print("Property 'price' and 'points' updated to their respective types successfully")
except Exception as e:
    print(f"Error updating property types: {e}")

# Consulta 1: Encuentra los vinos de Chile con más de 85 puntos y un precio menor a 20.
query_1 = """
MATCH (n:points)-[:RELATED_TO]->(c:vino)
WHERE c.country = 'Chile' AND n.points > 85 AND n.price < 20
RETURN c.designation AS designation, c.country AS country, n.points AS points, n.price AS price

"""

try:
    result_1 = conn.query(query_1)
    if result_1:
        for record in result_1:
            print(record)
    else:
        print("No results found")
except Exception as e:
    print(f"Query error: {e}")

# Cerrar la conexión
conn.close()
