import io
from logging import INFO, WARN, ERROR, Logger, DEBUG
from multiprocessing.sharedctypes import Value
import threading
from typing import AnyStr, List, Set, Tuple
import uuid

from tiny_etl.commons import dict_deep_get
from tiny_etl.loaders.commons import AbstractLoader

class CSV_FileLoader(AbstractLoader):
    def __init__(self, 
                logger: Logger, 
                input_key_path: List[AnyStr],
                values_path: List[Tuple[str, List[AnyStr], bool]],
                out_dir: str,
                col_sep: str=";",
                out_file_ext="txt",
                out_file_name_prefix="out_",
                buffer_size: int = 1000
                ):
        super().__init__(logger, input_key_path, values_path)
        self.out_dir=out_dir
        self.file_hd = None
        self.col_sep = col_sep
        self.out_file_ext = out_file_ext
        self.out_file_name_prefix = out_file_name_prefix
        self.calling_thread = Value('i', -1)
        self.buffer_size=buffer_size
        self.buffer = []
        self.uuid = str(uuid.uuid1())

    def _row_from_item(self, item: dict) -> List[AnyStr]:
        row = []
        for (title, key_path, required) in self.values_path:
            val = dict_deep_get(item, key_path)
            if required is not None and required is True and val is None:
                return None
            row.append(str(val))
        return row

    def load(self, job_uuid: str, items: List[dict], last_call: bool):

        id = threading.get_ident()
        if self.calling_thread.value==-1:
            self.calling_thread.value=id
        elif id != self.calling_thread.value:
            raise RuntimeError('Calling the same loader from diffrent threads')
        
        rows = []
        for item in items:
            x = dict_deep_get(item, self.input_key_path) if self.input_key_path is not None else item
            if x is not None:
                row = self._row_from_item(x)
                if row is not None:
                    rows.append(self.col_sep.join(row))

        if len(rows) >0: 
            self.buffer = self.buffer + rows

        if last_call or len(self.buffer) > self.buffer_size:
            self.write_buffered_data_to_disk()

    def _out_filename(self, job_uuid: str) -> str:
        return "{}_{}.{}".format(self.out_file_name_prefix, job_uuid, self.out_file_ext)

    def _open_file(self):
        import codecs
        import os

        if self.file_hd is None:
            file_name = self._out_filename(self.uuid)
            file_path = os.path.join(self.out_dir, file_name)
            self.file_hd = codecs.open(file_path, 'a', encoding = "utf-8")
            super().log_msg("File {} opened using the buffering {}bytes".format(file_path, io.DEFAULT_BUFFER_SIZE))
        return self.file_hd

    def write_buffered_data_to_disk(self):
        rows_nbr = len(self.buffer)  
        if rows_nbr>0:
            fhd = self._open_file()
            fhd.write("\n".join(self.buffer) + "\n")
            super().log_msg("{} total rows written in the file".format(rows_nbr))
            self.buffer.clear()

    def close(self) -> None:
        super().log_msg("Closing loader <>".format(__class__.__name__), level=INFO)
        try:
            if len(self.buffer) > 0:
                super().log_msg('Flushing buffered data in <{}>'.format(str(self.__class__.__name__)), level=INFO)
                self.write_buffered_data_to_disk()
                self.buffer.clear()
                super().log_msg('Flushed buffered data in <{}>'.format(str(self.__class__.__name__)), level=INFO)
            self.file_hd.flush()
            self.file_hd.close()
            super().log_msg("File closed successfully")
        except Exception as ex:
            super().log_msg("Error closing File handler", exception=ex , level=ERROR)

    def has_buffered_data(self) -> bool:
        return len(self.buffer)>0