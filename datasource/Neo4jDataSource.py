import logging
import neo4j

from neo4j import Result, GraphDatabase
from neo4j.graph import Node, Relationship
from neo4j.exceptions import DriverError, Neo4jError

from datasource.DataSourceAbstract import DataSourceAbstract

GET_DEFAULT_DB = "SHOW DEFAULT DATABASE"
GET_DBs = "SHOW DATABASES"
APOC_GRAPH_FROM_CYPHER = "CALL apoc.graph.fromCypher($query, $param, apoc.create.uuid(), {}) \
                        YIELD graph AS g \
                        RETURN g.nodes AS nodes, g.relationships AS rels";


class Neo4jDataSource(DataSourceAbstract):
    _logger = logging.getLogger("Neo4jDataSource")

    def __init__(self, protocol, uri, port, user, password):
        auth = (user, password) if user and password else None
        connection_uri = f"{protocol}://{uri}:{port or '7684'}"

        self.driver = GraphDatabase.driver(connection_uri, auth=auth)
        self.default_db = None

    def __del__(self):
        self.driver.close()

    def close(self):
        self.driver.close()

    def _session(self, database=None, bookmarks=(), access_mode=neo4j.WRITE_ACCESS, fetch_size=1000):
        database = database or self.default_db
        return self.driver.session(
            database=database, bookmarks=bookmarks,
            default_access_mode=access_mode, fetch_size=fetch_size
        )

    def get_default_graph(self):
        if not self.default_db:
            try:
                with self._session() as ssn:
                    query_result = ssn.run(GET_DEFAULT_DB)
                    return query_result.single()["name"]
            except DriverError as nErr:
                raise RuntimeError("an unexpected error occurred in the neo4j driver")
            except Exception:
                self.default_db = "neo4j"

    def get_graphs(self):
        databases = self.run_query(query=GET_DBs)
        return [database.get("name") for database in databases if database]

    def run_query(self, query: str, params: dict = {}, graph: str = None, write: bool = False,
                  result_as_df: bool = False):
        """
        :param result_as_df: if true returns the result as dataframe pands
        :param graph: the graph on which to execute the query
        :param query: the string containing the cypher query to be executed
        :param params: the parameters to be passed when executing the query
        :param write: if set to true indicates that the query is in write, otherwise it is read-only query
        :return: returns a list with the results of the query
        """
        graph = graph if graph else self.get_default_graph()
        self._logger.info(f"LOG - INFO [neoj4 - query]: running query on {graph} DB instance:\n {query}")
        try:
            with self._session(database=graph) as ssn:
                query_runner =  self._run_query_df if result_as_df else self._run_query_dict
                if not write:
                    result = ssn.read_transaction(
                        transaction_function=query_runner,
                        query=query, params=params
                    )
                else:
                    result = ssn.write_transaction(
                        transaction_function=query_runner,
                        query=query, params=params
                    )
                return result
        except (Neo4jError, DriverError) as nErr:
            if isinstance(nErr, Neo4jError):
                error = ValueError(f"the given neo4j query is not invalid: {nErr.message}")
            else:
                error = RuntimeError("InternalServerError -> error of Neo4j Driver")
            raise error


    def graph_from_query(self, query: str, params: dict = {}, graph: str = None):
        graph = graph or self.get_default_graph()
        print(f"LOG - INFO [neoj4_utils - query]: running query on {graph} DB instance: {query}")
        try:
            with self._session(database=graph) as ssn:
                result = ssn.read_transaction(
                    self._run_query_single,
                    APOC_GRAPH_FROM_CYPHER,
                    {'query': query, 'param': params}
                )
                nodes = [self._get_node_as_map(n, result.get("nodeMetadata")) for n in result.get("nodes")]
                rels = [self._get_edge_as_map(r) for r in result.get("rels")]
                return {'nodes': nodes, 'rels': rels}
        except (Neo4jError, DriverError) as nErr:
            if isinstance(nErr, Neo4jError):
                error = ValueError(f"the given neo4j query is not invalid: {nErr.message}")
            else:
                error = RuntimeError("InternalServerError -> error of Neo4j Driver")
            raise error


    """
        utility methods
    """

    @classmethod
    def _get_error_response(cls, err):
        if isinstance(err, DriverError):
            print(f"{err.__class__.__name__} occurred: {err}")
            error = {'errorCode': 500, 'message': str(err)}
        else:
            print(f"{err.__class__.__name__} occurred [{err.code}]: {err.message}")
            error = {'errorCode': err.code, 'message': err.message}
        return error

    @classmethod
    def _run_query_dict(cls, tx, query, params):
        res: Result = tx.run(query=query, parameters=params)
        return res.data()

    @classmethod
    def _run_query_df(cls, tx, query, params):
        res: Result = tx.run(query=query, parameters=params)
        return res.to_df()

    @classmethod
    def _run_query_single(cls, tx, query, params):
        res: Result = tx.run(query=query, parameters=params)
        return res.single()

    @classmethod
    def _get_node_as_map(cls, node: Node, metadata={}):
        node_map = {
            "id": node.id,
            "labels": node.labels,
            "properties": {k: v for (k, v) in node.items()},
            "nodeMetadata": metadata.get(str(node.id)) if metadata else {}
        }
        return node_map

    @classmethod
    def _get_edge_as_map(cls, edge: Relationship):
        edge_map = {
            "id": edge.id,
            "type": edge.type,
            "source": edge.start_node.id,
            "target": edge.end_node.id,
            "properties": {k: v for (k, v) in edge.items()}
        }
        for key in edge.keys():
            edge_map.get("properties")[key] = edge.get(key)
        return edge_map

