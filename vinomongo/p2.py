from pymongo import MongoClient
from pprintpp import pprint

client = MongoClient('mongodb://localhost:27017/')
db = client['wines_planet']


pipeline = [
    {
        '$group': {
            '_id': '$country',  # Agrupa por el campo 'country'
            'count': {'$sum': 1}  # Cuenta la cantidad de documentos por cada país
        }
    },
    {
        '$sort': {'count': -1}  # Ordena por cantidad en orden descendente
    }
]

result = list(db.idpo.aggregate(pipeline))


total_vinos = sum(doc['count'] for doc in result)


print("{:<30} {:<10} {:<5}".format('País', 'Cantidad', 'Porcentaje'))
print("=" * 60)
for doc in result:
    pais = doc['_id']
    cantidad = doc['count']
    porcentaje = (cantidad / total_vinos) * 100
    print("{:<30} {:<10} {:<10.4f}%".format(pais, cantidad, porcentaje))

print("=" * 60)
print("Total de vinos: {:16}".format(total_vinos))