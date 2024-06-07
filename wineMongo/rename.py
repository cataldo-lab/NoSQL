from pymongo import MongoClient

# Establece la conexión a MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["wines_world"]
coleccion = db["vinos"]

# Cambiar el nombre de la variable
resultado = coleccion.update_many(
    {},
    {"$rename": {"Unnamed: 0": "id"}}
)

print("Documentos modificados:", resultado.modified_count)

# Cerrar la conexión
client.close()
