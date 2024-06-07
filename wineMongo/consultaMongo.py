import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient

# Conexión al servidor local de MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['nombre_de_tu_base_de_datos']
collection = db['vinos']

# Función para convertir resultados a DataFrame
def query_to_df(query):
    results = list(collection.find(query))
    return pd.DataFrame(results)

# Consulta 1: Vinos con puntuación > 85 y precio < 20
df_query2 = query_to_df({"points": {"$gt": 85}, "price": {"$lt": 20}})
plt.figure(figsize=(10, 6))
plt.hist(df_query2['price'], bins=20, color='blue', alpha=0.7)
plt.title('Distribución de Precios de Vinos con Puntuación > 85 y Precio < 20 USD')
plt.xlabel('Precio ($)')
plt.ylabel('Frecuencia')
plt.grid(True)
plt.show()


#Consulta 2: Vinos Chilenos mejor evaluados:

vinos_altos = collection.find({"country": "Chile", "points": {"$gt": 85}})

print("Vinos chilenos con puntuación superior a 85:")
for vino in vinos_altos:
    print(f"{vino['title']} - Puntuación: {vino['points']}")



# Consulta 3: Vinos de Cabernet Sauvignon con notas de vainilla o roble
df_query6 = query_to_df({"variety": "Cabernet Sauvignon", "$or": [{"description": {"$regex": "vainilla"}}, {"description": {"$regex": "roble"}}]})
plt.figure(figsize=(10, 6))
plt.hist(df_query6['price'], bins=20, color='red', alpha=0.7)
plt.title('Precios de Vinos Cabernet Sauvignon con Notas de Vainilla o Roble')
plt.xlabel('Precio ($)')
plt.ylabel('Frecuencia')
plt.grid(True)
plt.show()

client.close()
