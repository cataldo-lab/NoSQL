import pandas as pd
import matplotlib.pyplot as plt
from py2neo import Graph

# Conexión al servidor local de Neo4j
graph = Graph("bolt://localhost:7687", auth=("neo4j", "tu_contraseña"))  # Cambia 'tu_contraseña' por la contraseña real

# Función para convertir resultados a DataFrame
def query_to_df(query):
    data = graph.run(query).data()
    return pd.DataFrame(data)

# Consulta 1: Vinos con puntuación > 85 y precio < 20
df_query2 = query_to_df("""
MATCH (v:Vino)
WHERE v.points > 85 AND v.price < 20
RETURN v.points AS points, v.price AS price
""")
plt.figure(figsize=(10, 6))
plt.hist(df_query2['price'], bins=20, color='blue', alpha=0.7)
plt.title('Distribución de Precios de Vinos con Puntuación > 85 y Precio < 20 USD')
plt.xlabel('Precio ($)')
plt.ylabel('Frecuencia')
plt.grid(True)
plt.show()


#Consulta 2: Vinos con puntuación > 85 
query = """
MATCH (v:Vino {country: 'Chile'})
WHERE v.points > 85
RETURN v.title AS nombre, v.points AS puntuacion
ORDER BY v.points DESC
"""
vinos_altos = graph.run(query)

print("Vinos chilenos con puntuación superior a 85:")
for vino in vinos_altos:
    print(f"{vino['nombre']} - Puntuación: {vino['puntuacion']}")



# Consulta 3: Vinos de Cabernet Sauvignon con notas de vainilla o roble
df_query6 = query_to_df("""
MATCH (v:Vino)
WHERE v.variety = 'Cabernet Sauvignon' AND (v.description CONTAINS 'vainilla' OR v.description CONTAINS 'roble')
RETURN v.price AS price
""")
plt.figure(figsize=(10, 6))
plt.hist(df_query6['price'], bins=20, color='red', alpha=0.7)
plt.title('Precios de Vinos Cabernet Sauvignon con Notas de Vainilla o Roble')
plt.xlabel('Precio ($)')
plt.ylabel('Frecuencia')
plt.grid(True)
plt.show()
