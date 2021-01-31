from multiprocessing import Pool
from functools import partial
import requests
from pymongo import MongoClient,UpdateOne
import feedparser
import json
import time
import logging
logging.getLogger().setLevel(logging.INFO)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("Task starting")

def fetch_content(id, feed):
    client = MongoClient()
    logging.info("New Mongo client for "+feed["link"])
    db = client.monolite
    collection = db.raw
    logging.info("HTTP Request for "+feed["link"])
    r = requests.get(feed['link'])
    logging.info("Done HTTP Request for "+feed["link"])
    data = {
            "source": id,
            "url": feed['link'],
            "desc": feed['title'],
            "raw": r.text,
            "utc": int(time.time())
        }
    collection.update_one({"url":feed['link']},{"$set":data},upsert=True)
    logging.info("Inserted "+feed["link"])
    client.close()
    return 0

client = MongoClient()
db = client.monolite
sources = db.sources.find()

for source in sources:
    logging.info("Source - "+source["media"])
    d = feedparser.parse(source["rss"])
    pool = Pool()
    func = partial(fetch_content,source['_id'])
    pool.map(func,d["entries"])
    pool.terminate()
