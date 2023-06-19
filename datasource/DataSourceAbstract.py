from abc import ABC, abstractmethod


class DataSourceAbstract(ABC):

    @abstractmethod
    def get_default_graph(self):
        ...

    @abstractmethod
    def get_graphs(self):
        ...

    @abstractmethod
    def run_query(self, query: str, params: dict = {}, graph: str = None, write: bool = False, result_as_df: bool = False):
        ...

    @abstractmethod
    def graph_from_query(self, query: str, params: dict = {}, graph: str = None):
        ...

    @abstractmethod
    def close(self):
        ...