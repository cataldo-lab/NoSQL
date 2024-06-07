import pandas as pd
from py2neo import Graph

# Conexión al servidor local de Neo4j
graph = Graph("bolt://localhost:7687", auth=("neo4j", "tu_contraseña"))

# Leer el archivo CSV
data = pd.read_csv('ruta_a_tu_archivo.csv')

# Crear nodos en Neo4j
#Agregar primera columna vacia
for index, row in data.iterrows():
    query = """
    CREATE (v:Vino {
      Id: $Id,
      country: $country,
      description: $description,
      points: $points,
      price: $price,
      province: $province,
      taster_name: $taster_name,
      variety: $variety
    })
    """
    graph.run(query, parameters=row.to_dict())

print('Datos insertados correctamente.')
