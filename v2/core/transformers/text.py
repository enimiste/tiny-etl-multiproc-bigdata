from abc import abstractmethod
from functools import reduce
from logging import Logger, ERROR, DEBUG, INFO
from typing import Any, AnyStr, Callable, Generator, Tuple, List
from core.transformers.commons import AbstractTransformer


class AbstractTextWordTokenizerTransformer(AbstractTransformer):
    def __init__(self, logger: Logger, 
                input_key_path: List[AnyStr],
                output_key: str,
                mappers: List[Callable[[Any], Any]] = [],
                remove_chars: List[AnyStr] = [],
                ignore_word_fn: Callable[[Any], bool] = None,
                copy_values_key_paths: List[Tuple[str, List[AnyStr]]] = None,
                remove_key_paths: List[List[AnyStr]]=None) -> None:
        """
        Yield {'word': str}
        """
        super().__init__(logger, input_key_path, (str), output_key, copy_values_key_paths, remove_key_paths)
        self.mappers = mappers
        self.ignore_word_fn = ignore_word_fn
        self.remove_chars = remove_chars

    def _map_item(self, text: str, context: dict = {}) -> Generator[dict, None, None]:
        if text is None:
            super().log_msg("Item value is None")
            return IgnoreTransformationResult

        for x in self._tokenize_text(text, context):
            for car in self.remove_chars:
                if car is not None:
                    x = x.replace(car, '')
                    
            x = reduce(lambda val, uvn: uvn(val), self.mappers, x)
            if x is not None and (self.ignore_word_fn is None or not self.ignore_word_fn(x)):
                yield {'word': x}

    @abstractmethod
    def _tokenize_text(self, text: str, context: dict) -> Generator[List[AnyStr], None, None]:
        pass

class TextWordTokenizerTransformer(AbstractTextWordTokenizerTransformer):
    def __init__(self, logger: Logger, 
                pattern: str, 
                input_key_path: List[AnyStr],
                output_key: str,
                mappers: List[Callable[[Any], Any]] = [],
                remove_chars: List[AnyStr] = [],
                ignore_word_fn: Callable[[Any], bool] = None,
                copy_values_key_paths: List[Tuple[str, List[AnyStr]]] = None,
                remove_key_paths: List[List[AnyStr]]=None) -> None:
        """
        pattern               : str
        input_key_path        : List[in_path]
        output_key            : str
        mappers               : List[Callable[[word], new_value]]
        remove_chars          : List[char_to_be_removed] it will be called before mappers
        ignore_word_fn        : Callable[[word], bool],
        copy_values_key_paths : List[Tuple[out_path, List[in_path]]]
        remove_key_paths      : List[List[in_path]]

        Yield {'word': str}
        """
        super().__init__(logger, input_key_path, output_key, mappers, remove_chars,ignore_word_fn, copy_values_key_paths, remove_key_paths)
        self.pattern = pattern

    def _tokenize_text(self, text: str, context: dict) -> Generator[str, None, None]:
        import re
        for x in re.split(self.pattern, text):
            yield x
