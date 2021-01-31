from multiprocessing import Pool
import requests
from pymongo import MongoClient
from bson.objectid import ObjectId
import json
import time
import logging

client = MongoClient()
client_new = MongoClient()
db = client.monolite_old
db_new = client.monolite

SOURCES = [  
    {
        "name":"nytimes.com",
        "id":"601106c1a6eef273b5723f23"
    },
    {
        "name":"cnn.com",
        "id":"601106c1a6eef273b5723f24"
    },
    {
        "name":"cnbc.com",
        "id":"601106c1a6eef273b5723f25"
    },
    {
        "name":"bbci.co.uk",
        "id":"601106c1a6eef273b5723f26"
    },
    {
        "name":"rthk9.rthk.hk",
        "id":"601106c1a6eef273b5723f27"
    },
    {
        "name":"independent.co.uk",
        "id":"601106c1a6eef273b5723f28"
    }
]


for source in SOURCES:
    print("LOOKING FOR ARTICLES FOR "+source['name'])
    articles = db.raw.find({"url" : {"$regex" : ".*" + source['name'] + ".*"}})
    print("FOUND " + str(articles.count()) + " FOR " + source['name'])
    for article in articles:
        article['source'] = ObjectId(source['id'])
        x = db_new.raw.insert(article)
        print("INSERTED 1 ARTICLE FOR " + source['name'])