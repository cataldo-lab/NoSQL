from pymongo import MongoClient
from pprintpp import pprint

client = MongoClient('mongodb://localhost:27017/')
db = client['wines_planet']

pipeline = [
    {
        '$lookup': {
            'from': 'idpo',
            'localField': 'PII',
            'foreignField': 'PII',
            'as': 'idpo_docs'
        }
    },
    {
        '$unwind': '$idpo_docs'
    },
    {
        '$match': {
            'idpo_docs.country': 'Chile',
            'idpo_docs.points': {'$ne': ''},  #Filtra este campo vacio
            'idpo_docs.price': {'$ne': ''}    #Filtra este campo vacio
        }
    },
    
    {
        '$addFields': {
            'idpo_docs.points': {'$toDouble': '$idpo_docs.points'},
            'idpo_docs.price': {'$toDouble': '$idpo_docs.price'}
        }
    },
    {
      '$match': {
          'idpo_docs.points': {'$gte': 85},
          'idpo_docs.price': {'$lt': 20}
      }  
    },
    {
        '$limit': 10
    },
    {
        '$project': {
            '_id': 0,
            'PII': 1,
            'designation': 1,
            'idpo_docs.country': 1,
            'idpo_docs.points': 1,
            'idpo_docs.price': 1
        }
    },
    {
        '$sort': {'idpo_docs.points': 1}
    }
]

result = list(db.vino.aggregate(pipeline))

pprint(result)