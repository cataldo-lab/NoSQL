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
        '$match': {
            'idpo_docs.price': {'$gt': 90.0}  
        }
    },
    {
        '$sort': {'idpo_docs.price': -1}  
    },
    {
        '$limit': 1  
    },
    {
        '$project': {
            '_id': 0,
            'PII': '$idpo_docs.PII',
            'country': '$idpo_docs.country',
            'price': '$idpo_docs.price'
        }
    }
]


result = list(db.vino.aggregate(pipeline))


pprint(result)
