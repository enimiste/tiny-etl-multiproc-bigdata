from logging import INFO, WARN, ERROR, Logger, DEBUG
from multiprocessing.sharedctypes import Value
import threading
from typing import AnyStr, List, Set, Tuple

from core.commons import dict_deep_get
from core.loaders.commons import AbstractLoader

from cassandra.cluster import Session

class Cassandra_DBLoader(AbstractLoader):
    def __init__(self, 
                logger: Logger, 
                input_key_path: List[AnyStr],
                values_path: List[Tuple[str, List[AnyStr], bool]],
                buffer_size: int,
                sql_query: str,
                db_name: str,
                hosts: List[AnyStr] = ['127.0.0.1'],
                request_timeout=15,
                executor_threads=2 ):
        super().__init__(logger, input_key_path, values_path)
        self.buffer_size=buffer_size
        self.buffer = []
        self.calling_thread = Value('i', -1)
        self.sql_query = sql_query
        self.db_name = db_name
        self.hosts = hosts
        self.prepared_query = None
        self.session = None
        self.request_timeout=request_timeout
        self.executor_threads=executor_threads

    def _row_from_data(self, item: dict)->list:
        row = []
        for (title, key_path, required) in self.values_path:
            val = dict_deep_get(item, key_path)
            if required is not None and required is True and val is None:
                return None
            row.append(val)
        return row


    def load(self, job_uuid: str, items: List[dict], last_call: bool) -> None:
        id = threading.get_ident()
        if self.calling_thread.value==-1:
            self.calling_thread.value=id
        elif id != self.calling_thread.value:
            raise RuntimeError('Calling the same loader from diffrent threads')

        data = []
        for item in items:
            x = dict_deep_get(item, self.input_key_path) if self.input_key_path is not None else item
            if x is not None:
                d = self._row_from_data(x)
                if d is not None:
                    data.append(d)

        if len(data)>0:
            self.buffer = self.buffer + data

        if last_call or (len(self.buffer) > self.buffer_size):
            self.write_buffered_data_to_disk()

    def _connect(self) -> Session:
        from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT, Session
        from cassandra.policies import WhiteListRoundRobinPolicy, DowngradingConsistencyRetryPolicy, ConsistencyLevel
        from cassandra.query import tuple_factory

        if self.session is None or self.cluster is None or self.cluster.is_shutdown():

            profile = ExecutionProfile(
                load_balancing_policy=WhiteListRoundRobinPolicy(self.hosts),
                retry_policy=DowngradingConsistencyRetryPolicy(),
                consistency_level=ConsistencyLevel.LOCAL_QUORUM,
                serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
                request_timeout=self.request_timeout,
                row_factory=tuple_factory
            )
            self.cluster = Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile}, executor_threads=self.executor_threads)
            self.session = self.cluster.connect(keyspace=self.db_name)
            super().log_msg('Connection was successfully opened to Cassandra')
            self.prepared_query = self.session.prepare(self.sql_query)

        return self.session

    def write_buffered_data_to_disk(self) -> None:

        session = self._connect()
        items =  [] + self.buffer
        data_len=len(items)
        super().log_msg("{0} rows available to be inserted".format(data_len))
        for row in self.buffer:
            session.execute(self.prepared_query, row)
        super().log_msg("{} Total record inserted successfully".format(data_len))
        self.buffer.clear()

    def close(self) -> None:
        try:
            if len(self.buffer) > 0:
                super().log_msg('Flushing buffered data in <{}>'.format(str(self.__class__.__name__)), level=INFO)
                self.write_buffered_data_to_disk()
                self.buffer.clear()
                super().log_msg('Flushed buffered data in <{}>'.format(str(self.__class__.__name__)), level=INFO)
            if self.connection is not None:
                self.cluster.shutdown()
            super().log_msg("Cassandra connection is closed successfully",  level=INFO)
        except Exception as ex:
            super().log_msg("Error closing Cassandra connection", exception=ex , level=ERROR)

    def has_buffered_data(self) -> bool:
        return len(self.buffer)>0
            


