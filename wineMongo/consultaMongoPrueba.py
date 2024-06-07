import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient

# Conexión al servidor local de MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['wines_world']
collection = db['vinos']

# Función para convertir resultados a DataFrame
def query_to_df(query):
    results = list(collection.find(query))
    return pd.DataFrame(results)

# Consulta 1: Vinos con puntuación > 85 y precio < 20
vinos_baratos = collection.find({"points": {"$gt": 85}, "price": {"$lt": 20}}).sort("price, 1").limit(10)
for vino in vinos_baratos:
    # Asegúrate de que cada clave exista en el documento, de lo contrario, imprime un valor predeterminado
    country = vino.get("country", "No especificado")
    points = vino.get("points", "No especificado")
    price= vino.get("price", "No especificado")
    province = vino.get("province", "No especificado")
    variety = vino.get("variety", "No especificado")

    print("{},{},{},{},{}".format(country, points, price, province, variety))

#Consulta 2: Vinos Chilenos mejor evaluados en orden decreciente:

print("______________________________")
print("Consulta 2: Vinos Chilenos mejor evaluados en orden decreciente")
print("Vinos chilenos con puntuación superior a 85")
vinos_altos = collection.find({"country": "Chile", "points": {"$gt": 85}}).sort("points,-1")


for vino in vinos_altos:
    # Asegúrate de que cada clave exista en el documento, de lo contrario, imprime un valor predeterminado
    country = vino.get("country", "No especificado")
    points = vino.get("points", "No especificado")
    price= vino.get("price", "No especificado")
    province = vino.get("province", "No especificado")
    variety = vino.get("variety", "No especificado")
    
    
    print("{},{},{},{},{}".format(country, points, price, province, variety))


print("______")
print("Top 10 vinos, uno por país, con la mejor puntuación:")

pipeline = [
    {"$match": {"points": {"$gt": 92}}},  # Filtra vinos con más de 92 puntos
    {"$sort": {"country": 1, "points": -1}},  # Ordena por país y luego por puntos de manera descendente
    {"$group": {  # Agrupa por país
        "_id": "$country",
        "country":{"$first": "$country"},
        "points": {"$first": "$points"},
        "price": {"$first": "$price"},
        "province": {"$first": "$province"},
        "variety": {"$first": "$variety"}
       
    }},
    {"$sort": {"points": -1}},  # Opcional: Ordena todos los países por puntuación para priorizar los mejores
    {"$limit": 10}  # Limita los resultados a 10
]

vinos_top = collection.aggregate(pipeline)

# Imprimir los mejores vinos por país
for vino in vinos_top:
    country = vino.get("country", "No especificado")
    points = vino.get("points", "No especificado")
    price = vino.get("price", "No especificado")
    province = vino.get("province", "No especificado")
    variety = vino.get("variety", "No especificado")
    
    print("{},{},{},{},{}".format(country, points, price, province, variety))


#Consulta 4: La cantidad de vinos producidas por todos los países en un grafico de torta
print("______________________________")


client.close()
