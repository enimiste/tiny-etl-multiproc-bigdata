from abc import abstractmethod
from logging import Logger, ERROR, DEBUG, INFO
from typing import Any, AnyStr, Generator, Tuple, List
from tiny_etl.commons import WithLogging
from tiny_etl.commons import dict_deep_get
from tiny_etl.commons import dict_deep_remove

IgnoreTransformationResult = object()

class AbstractTransformer(WithLogging):
    def __init__(self, logger: Logger, 
                input_key_path: List[AnyStr], 
                input_value_type: Any,
                output_key: str,
                copy_values_key_paths: List[Tuple[str, List[AnyStr]]] = None,
                remove_key_paths: List[List[AnyStr]]=None) -> None:
        super().__init__(logger)
        self.input_key_path = input_key_path
        self.output_key = output_key
        self._input_value_type = input_value_type
        self.copy_values_key_paths = copy_values_key_paths
        self.remove_key_paths = remove_key_paths

    @staticmethod
    def _copy_input_values_to_output(copy_values_key_paths: List[Tuple[str, List[AnyStr]]], dest: dict, source: dict):
        if copy_values_key_paths is not None:
            for (key, path) in copy_values_key_paths:
                x = dict_deep_get(source, path)
                if x is not None:
                    dest[key] = x
        return dest

    def transform(self, item: dict, context: dict={}) -> Generator[dict, None, None]:
        if item is None:
            return None
        input_value = dict_deep_get(item, self.input_key_path) if self.input_key_path is not None else item
        
        if input_value is None:
            raise RuntimeError("Item doesn't contains the input_key_path={}".format('.'.join(self.input_key_path)))

        if not isinstance(input_value, self._input_value_type):
            raise RuntimeError("Input value expected type : {}, is different from the given one : {}".format(self._input_value_type, type(input_value)))
        
        context['__input_item__'] = item
        for res in self._map_item(input_value, context):
            if res != IgnoreTransformationResult:
                item_ = {}
                item_ = AbstractTransformer._copy_input_values_to_output(self.copy_values_key_paths, item_, item)
                if self.remove_key_paths is not None:
                    for remove_key_path in self.remove_key_paths:
                        dict_deep_remove(item, remove_key_path)
                item_[self.output_key] = res
                yield item_
            else:
                super().log_msg("Result ignored <IgnoreTransformationResult> : {}".format(str(res)))
        if "__input_item__" in context:
            del context["__input_item__"]

    @abstractmethod
    def _map_item(self, item, context: dict={}) -> Generator[dict, None, None]:
        pass

    def close(self) -> None:
        super().log_msg("Closing loader <>".format(__class__.__name__), level=INFO)

    def set_input_key_path(self, v: List[AnyStr]):
        self.input_key_path = v

    def set_output_key(self, v: str):
        self.output_key = v


class OneToOneNoopTransformer(AbstractTransformer):
    def __init__(self, logger: Logger, log: bool = False, log_level = DEBUG, log_prefix: str='') -> None:
        """
        It is transparent,
        Yields the same input without any changes
        """
        super().__init__(logger, None, None, None, None)
        self.log = log
        self.log_level = log_level
        self.log_prefix = log_prefix

    def transform(self, item: dict, context: dict={}) -> Generator[dict, None, None]:
        if self.log:
            super().log_msg("{} {}".format(self.log_prefix, str(item)),level=self.log_level)
        yield item

    def _map_item(self, item, context: dict = {}) -> Generator[dict, None, None]:
        yield item
