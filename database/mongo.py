from threading import RLock
from pymongo import MongoClient
from config import DB_URI

def start():
    client = MongoClient(DB_URI)
    return client["telegram"]

SESSION = start()
INSERTION_LOCK = RLock()

async def add_user(id, username):
    collection = SESSION["user"]
    with INSERTION_LOCK:
        msg = collection.find_one({"id": id, "user_name": username})
        if msg is None:
            collection.insert_one({"id": id, "user_name": username})
        else:
            pass

async def full_userbase():
    collection = SESSION["user"]
    users = collection.find()
    return users

async def query_msg():
    collection = SESSION["user"]
    query = collection.find().distinct("id")
    return query