from config import Config
import pymongo


def connect_to_db():
    client_db = pymongo.MongoClient(Config.HOST, Config.PORT)
    client_db[Config.DBAUTHDB].authenticate(Config.DBUSERNAME, Config.DBPWD)
    return client_db