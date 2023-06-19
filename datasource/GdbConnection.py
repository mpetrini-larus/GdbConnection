import logging

from datasource.GremlinDataSource import GremlinDataSource
from datasource.Neo4jDataSource import Neo4jDataSource
from datasource.MongoDbDataSource import MongoDbDataSource
from datasource.metaclass.SingletonConnection import SingletonConnection


class GdbConnection(metaclass=SingletonConnection):

    _logger = logging.getLogger("GdbConnection")

    def __init__(self, mongo=None):
        self.connection_config: dict = {}
        self.driver = None
        self.connected = False
        self.mongo = mongo or MongoDbDataSource()

    def check_driver(self):
        if self.driver is None: raise ValueError("the driver instance is misconfigured")
        try:
            if isinstance(self.driver, Neo4jDataSource):
                self.driver.driver.verify_connectivity()
            elif isinstance(self.driver, GremlinDataSource):
                t = self.driver._get_traversal(self.driver.get_default_graph())
                t.V().limit(1).count().next()
            else:
                raise Exception("the driver instance is misconfigured")
        except Exception as ex:
            self.connected = False
            raise ValueError(str(ex))

    def get_graph_data_source(self):
        if self.driver is None or not self.connected:
            graph_connection_config: dict = self.get_graph_connection()

            if graph_connection_config.get("type").upper() == "NEO4J":
                driver = Neo4jDataSource(
                    protocol=graph_connection_config.get("protocol"),
                    uri=graph_connection_config.get("uri"),
                    port=graph_connection_config.get("port"),
                    user=graph_connection_config.get("user"),
                    password=graph_connection_config.get("pass")
                )
            elif graph_connection_config.get("type").upper() in ["JANUSGRAPH", "COSMOSDB"]:
                driver = GremlinDataSource(
                    protocol=graph_connection_config.get("protocol"),
                    host=graph_connection_config.get("uri"),
                    port=graph_connection_config.get("port"),
                    user=graph_connection_config.get("user"),
                    password=graph_connection_config.get("pass")
                )
            else:
                raise ValueError("unexpected.connector.type")

            self.connection_config = graph_connection_config
            try:
                self.driver = driver
                self.check_driver()
                self.connected = True
            except ValueError:
                del self.driver
                self.driver = None
                self.connected = False
        return self.driver

    def get_graph_connection(self):
        if not self.connected:
            connections = list(self.mongo.find_all("graphDbConnection"))
            if connections is None or len(connections) != 1 or not isinstance(connections, list):
                self._logger.error("graphdb.connection.misconfiguration")
                raise ValueError("graphdb.connection.misconfiguration")

            connection = connections[0]
            if not connection.get("type"):
                raise ValueError("graphdb.connection.misconfiguration")
            if not connection.get("uri"):
                raise ValueError("graphdb.connection.missingUriOrProtocol")
            if connection.get("type") in ["COSMOSDB", "JANUSGRAPH"]:
                if not connection.get("tinkerpopGraphs"):
                    raise ValueError("graphdb.connection.misconfiguration")
            elif not connection.get("protocol"):
                raise ValueError("graphdb.connection.missingUriOrProtocol")
            self.connection_config = connection

        return self.connection_config

    @classmethod
    def evict_singleton_instance(cls):
        SingletonConnection.evict_instance(GdbConnection)
