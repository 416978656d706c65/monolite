from multiprocessing import Pool
import requests
from pymongo import MongoClient,UpdateOne
import feedparser
import json
import time
import logging
logging.getLogger().setLevel(logging.INFO)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("Task starting")

def fetch_content(feed):
    client = MongoClient()
    logging.info("New Mongo client for "+feed["link"])
    db = client.monolite
    collection = db.raw
    logging.info("HTTP Request for "+feed["link"])
    r = requests.get(feed['link'])
    logging.info("Done HTTP Request for "+feed["link"])
    data = {
            "url": feed['link'],
            "desc": feed['title'],
            "raw": r.text,
            "utc": int(time.time())
        }
    collection.update_one({"url":feed['link']},{"$set":data},upsert=True)
    logging.info("Inserted "+feed["link"])
    client.close()
    return 0

with open('/home/orch/monolite/scraper/sources.list.json') as json_file:
    sources = json.load(json_file)
    logging.info("Running for: ")
    for source in sources:
        logging.info("Source - "+source["media"])
        d = feedparser.parse(source["rss"])
        pool = Pool()
        pool.map(fetch_content,d["entries"])
        pool.terminate()
