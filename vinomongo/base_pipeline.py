from pymongo import MongoClient
from datetime import datetime, timedelta

client = MongoClient('mongodb://localhost:27017/')
db = client['wines_planet']

pipeline = [
]

consulta = list(db.vino.aggregate(pipeline))
print(consulta)