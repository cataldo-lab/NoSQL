from pymongo import MongoClient
from pprintpp import pprint


client = MongoClient('mongodb://localhost:27017/')
db = client['wines_planet']


db.vino.create_index('PII')
db.idpo.create_index('PII')


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
            'idpo_docs.price': {'$ne': ''}  
        }
    },
    {
        '$addFields': {
            'idpo_docs.price': {'$toDouble': '$idpo_docs.price'}  
        }
    },
    {
        '$group': {
            '_id': '$idpo_docs.country',  
            'max_price': {'$max': '$idpo_docs.price'},  
            'wine': {'$first': '$$ROOT'}  
        }
    },
    {
        '$sort': {'max_price': -1}  
    },
    {
        '$project': {
            '_id': 0,
            'country': '$_id',
            'PII': '$wine.idpo_docs.PII',
            'designation': '$wine.idpo_docs.designation',
            'price': '$max_price',
            
        }
    },
  
]

result = list(db.vino.aggregate(pipeline))

pprint(result)
