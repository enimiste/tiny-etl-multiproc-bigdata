from logging import Logger
from typing import Generator

from core.transformers import AbstractTextWordTokenizerTransformer, AbstractTransformer

class ArabicTextWordsTokenizerTransformer(AbstractTextWordTokenizerTransformer):
    def __init__(self, logger: Logger, item_key: str = '_') -> None:
        super().__init__(logger, item_key)

    def _tokenize_text(self, text: str, item: dict, context: dict) -> Generator[list[dict], None, None]:
        import re
                    
        arabic_words = re.findall(r'[َُِْـًٌٍّؤائءآىإأبتثجحخدذرزسشصضطظعغفقكلمنهـوي]+', text)
        for txt in arabic_words:
            words = txt.replace('×', '').replace(' ', '\n').replace('\r', '\n').replace('\t', '\n').split('\n')
            for w in words:
                if w and w.strip():
                    yield AbstractTransformer.updateItem(item, self.item_key, w)

class ArabicRemoveDiacFromWordTransformer(AbstractTransformer):
    def __init__(self, logger: Logger, item_key: str = '_') -> None:
        super().__init__(logger, item_key)

    def _map_item(self, item: dict, context: dict = {}) -> Generator[dict, None, None]:
        if not self.item_key in item:
            super().log_msg("Key {} not found in the item dict".format(self.item_key))
            return
        text = item[self.item_key]

        if text is None:
            super().log_msg("Item value is None")
            return

        text = text.replace('َ', '').replace('ّ', '').replace('ِ', '').replace('ُ', '').replace('ْ', '').replace('ً', '').replace('ٌ', '').replace('ٍ', '')
        
        yield AbstractTransformer.updateItem(item, self.item_key, text)