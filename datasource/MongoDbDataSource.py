from os import environ

from pymongo import MongoClient

from datasource.metaclass.SingletonMongoConnection import SingletonMongoConnection

MONGO_COLLECTIONS = ["graphDbConnection"]
MONGO_DB = "galileo"
MONGO_PORT = "27017"
MONGO_ADDRESS = "localhost"


class MongoDbDataSource(metaclass=SingletonMongoConnection):

    def __init__(self):
        self.client = MongoClient(self.get_mongo_uri(), self.get_mongo_port())
        global MONGO_DB
        self.db = self.client.get_database(MONGO_DB)

    def __del__(self):
        if self.client:
            self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.client.__exit__(exc_type, exc_value, traceback)

    def get_client(self):
        return self.client

    def get_collection(self, collection):
        if collection in MONGO_COLLECTIONS:
            return self.db.get_collection(collection)
        else:
            raise ValueError(f"value of 'collection' must be in [{MONGO_COLLECTIONS}]")

    def insert_one(self, collection, doc_to_write):
        target_collection = self.get_collection(collection)
        result = target_collection.insert_one(doc_to_write)
        return result

    def update_one(self, collection, query_filter, update):
        target_collection = self.get_collection(collection)
        result = target_collection.update_one(query_filter, {'$set': update})
        return result

    def update_one_push_element(self, collection, query_filter, update):
        target_collection = self.get_collection(collection)
        result = target_collection.update_one(query_filter, {'$push': update})
        return result

    def upsert_one(self, collection, query_filter, document):
        target_collection = self.get_collection(collection)
        result = target_collection.replace_one(query_filter, document, True)
        return result

    def delete_one(self, collection, query_filter):
        target_collection = self.get_collection(collection)
        result = target_collection.delete_one(query_filter)
        return result

    def find_all(self, collection):
        target_collection = self.get_collection(collection)
        result = target_collection.find({})
        return result

    def delete_all(self, collection):
        self.get_collection(collection).delete_many({})

    def find_one(self, collection, doc_id):
        target_collection = self.get_collection(collection)
        result = target_collection.find_one({"_id": doc_id})
        return result

    @classmethod
    def get_mongo_port(cls):
        mongo_port = environ.get("MONGO_PORT") if environ.get("MONGO_PORT") and environ.get("MONGO_PORT") != " " \
            else MONGO_PORT
        return int(mongo_port)

    @classmethod
    def get_mongo_uri(cls):
        if environ.get("MONGO_USERNAME") and environ.get("MONGO_USERNAME") != " ":
            username = environ.get("MONGO_USERNAME")
        else:
            raise ValueError("value of 'MONGO_USERNAME' environment var must be not None")

        if environ.get("MONGO_PASSWORD") and environ.get("MONGO_PASSWORD") != " ":
            password = environ.get("MONGO_USERNAME")
        else:
            raise ValueError("value of 'MONGO_PASSWORD' environment var must be not None")

        address = environ.get("MONGO_ADDRESS") if environ.get("MONGO_ADDRESS") and environ.get("MONGO_ADDRESS") != " " \
            else MONGO_ADDRESS
        return f"mongodb://{username}:{password}@{address}"

    @classmethod
    def evict_singleton_instance(cls):
        SingletonMongoConnection.evict_instance(MongoDbDataSource)
