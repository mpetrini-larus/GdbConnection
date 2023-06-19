import os
import unittest

from testcontainers.neo4j import Neo4jContainer
from testcontainers.mongodb import MongoDbContainer
from neo4j import GraphDatabase
from pymongo import MongoClient

from datasource.MongoDbDataSource import MongoDbDataSource
from datasource.Neo4jDataSource import Neo4jDataSource
from datasource.GdbConnection import GdbConnection


class MyTestCase(unittest.TestCase):
    mongo_container: MongoDbContainer = None
    neo4j_container: Neo4jContainer = None

    @classmethod
    def setUpClass(cls):
        print("starting neo4j test-container")
        cls.neo4j_container = Neo4jContainer('neo4j:4.4.11').with_env("NEO4JLABS_PLUGINS", """["apoc"]""")
        cls.neo4j_container.start()
        print("\n starting mongodb test-container")
        cls.mongo_container = MongoDbContainer('mongo:5.0.5')
        cls.mongo_container.start()

        # set env-var for mongo-data-source object
        os.environ["MONGO_PASSWORD"] = "test"
        os.environ["MONGO_USERNAME"] = "test"
        os.environ["MONGO_PORT"] = cls.mongo_container.get_exposed_port(27017)
        os.environ["MONGO_ADDRESS"] = "localhost"

        # set neo4j connection info on mongo
        with MongoClient(cls.mongo_container.get_connection_url()) as client:
            db = client["galileo"]
            db.get_collection("graphDbConnection").delete_many({})
            db.get_collection("graphDbConnection").insert_one({
                "protocol": "bolt",
                "uri": "localhost",
                "port": cls.neo4j_container.get_exposed_port(7687),
                "user": "neo4j",
                "pass": "password",
                "type": "NEO4J"
            })

        with GraphDatabase.driver(cls.neo4j_container.get_connection_url(), auth=("neo4j", "password")) as driver:
            with driver.session() as session:
                session.run("CREATE (n:Alchemist {name: 'Edward'})-[:BROTHER_OF]->(m: Alchemist {name: 'Alphonse'})")

    @classmethod
    def tearDownClass(cls):
        cls.neo4j_container.stop()
        cls.mongo_container.stop()
        MongoDbDataSource.evict_singleton_instance()
        GdbConnection.evict_singleton_instance()

    def test_gdbConnection(self):
        connection = GdbConnection()
        self.assertIsNotNone(connection)
        self.assertIsNotNone(connection.mongo)

        data_source = connection.get_graph_data_source()
        self.assertIsNotNone(data_source)
        self.assertTrue(isinstance(data_source, Neo4jDataSource))
        self.assertEqual("neo4j", data_source.get_default_graph())

        alchemist = data_source.run_query("MATCH (n:Alchemist {name: $name}) RETURN n", params={'name': 'Alphonse'})
        self.assertIsNotNone(alchemist)
        self.assertEqual(alchemist.get("name"), "Alphonse")



if __name__ == '__main__':
    unittest.main()
