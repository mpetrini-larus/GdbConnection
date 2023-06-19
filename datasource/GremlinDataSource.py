import logging
import ssl
from gremlin_python.driver.aiohttp.transport import AiohttpTransport
from gremlin_python.driver.client import Client
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.anonymous_traversal import traversal

from datasource.DataSourceAbstract import DataSourceAbstract


class GremlinDataSource(DataSourceAbstract):

    _logger = logging.getLogger("Neo4jDataSource")
    _CONNECTION_STRING: str = "{protocol}://{host}:{port}/gremlin"

    def __init__(
            self, protocol: str = None, host: str = "localhost",
            port: str = "8182", user: str = "", password: str = "",
            tinkerpop_graphs: dict = {}
    ):
        connection_string = self._CONNECTION_STRING.format(protocol=protocol, host=host, port=port)
        self.tinkerpop_graphs = tinkerpop_graphs
        self._logger.info(f"============>GREMLIN INIT CLIENTS<============")
        self.clients: dict = {
            key: self._init_clients(
                traversal_source=value, connection_string=connection_string,
                user=user, password=password
            )
            for key, value in tinkerpop_graphs.items() if value
        }
        self._logger.info(f"============>GREMLIN INIT CONNECTIONS<============")
        self._connections: dict = {
            key: self._init_connection(
                traversal_source=value, connection_string=connection_string,
                user=user, password=password
            )
            for key, value in tinkerpop_graphs.items() if value
        }
        self._logger.info(f"============>GREMLIN INIT TRAVERSALS<============")
        self.traversals: dict = {
            key: traversal().with_remote(value)
            for key, value in self._connections.items() if value
        }

    def __del__(self):
        for client in self.clients.values():
            client.close()
        for connection in self._connections.values():
            connection.close()

    def close(self):
        for client in self.clients.values(): client.close()
        for connection in self._connections.values(): connection.close()

    def _get_traversal(self, graph: str):
        if self.traversals:
            return self.traversals.get(graph)

    def _get_client(self, graph: str) -> Client:
        if self.clients:
            return self.clients.get(graph)

    @classmethod
    def _init_connection(
            cls, connection_string: str = "ws://localhost:{8182}/gremlin",
            user: str = "", password: str = "", traversal_source: str = "g"
    ) -> DriverRemoteConnection:

        transport_factory = None
        if not user and not password:
            transport_factory = lambda: AiohttpTransport(ssl_options=ssl.create_default_context(ssl.Purpose.CLIENT_AUTH))
        return DriverRemoteConnection(
            url=connection_string, traversal_source=traversal_source,
            username=user, password=password,
            transport_factory=transport_factory
        )

    @classmethod
    def _init_clients(
            cls, connection_string: str = "ws://localhost:{8182}/gremlin",
            user: str = "", password: str = "", traversal_source: str = "g"
    ) -> Client:
        return Client(url=connection_string, traversal_source=traversal_source, username=user, password=password)

    def run_query(
            self, query: str, params: dict = {}, graph: str = None,
            write: bool = False, result_as_df: bool = False
    ):
        if result_as_df:
            """
                if the query returns a list of dictionaries, 
                you could use `DataFrame.from_dict(vm)` to generate the pandas DataFrame
            """
            raise NotImplementedError("Gremlin query result conversion into a pandas data frame has not yet been implemented")
        graph_client: Client = self._get_client(graph)
        if graph_client:
            return graph_client.submit(message=query).all().response()
        else:
            raise KeyError("the selected graph is not available")

    def get_default_graph(self):
        return self.tinkerpop_graphs.keys()[0]

    def get_graphs(self):
        return self.tinkerpop_graphs.keys()

    def graph_from_query(self, query: str, params: dict = {}, graph: str = None):
        raise NotImplementedError("the method has not yet been implemented")



