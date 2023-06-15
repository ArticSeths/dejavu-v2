from time import time
from typing import Dict

import dejavu.logic.decoder as decoder
import numpy
from dejavu.base_classes.base_recognizer import BaseRecognizer
from dejavu.config.settings import (ALIGN_TIME, FINGERPRINT_TIME, QUERY_TIME,
                                    RESULTS, TOTAL_TIME, AUDIO_DURATION)


class FileRecognizer(BaseRecognizer):
    def __init__(self, dejavu):
        super().__init__(dejavu)

    def recognize_file(self, filename: str) -> Dict[str, any]:
        channels, self.Fs, _, audio_duration = decoder.read(filename, self.dejavu.limit)

        t = time()
        matches, fingerprint_time, query_time, align_time = self._recognize(*channels)
        t = time() - t

        if matches:
            for match in matches:
                match = {k: v.item() if isinstance(v, numpy.int64) else v for k, v in match.items()}

        results = {
            TOTAL_TIME: t,
            AUDIO_DURATION: audio_duration,
            FINGERPRINT_TIME: fingerprint_time,
            QUERY_TIME: query_time,
            ALIGN_TIME: align_time,
            RESULTS: matches
        }

        return results

    def recognize(self, filename: str) -> Dict[str, any]:
        return self.recognize_file(filename)
