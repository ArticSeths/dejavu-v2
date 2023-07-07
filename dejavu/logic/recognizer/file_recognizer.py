from time import time
from typing import Dict, Any
import os

from pydub import AudioSegment
import concurrent.futures
import numpy as np
import datetime

import dejavu.logic.decoder as decoder
import numpy
from dejavu.base_classes.base_recognizer import BaseRecognizer
from dejavu.config.settings import (ALIGN_TIME, FINGERPRINT_TIME, QUERY_TIME,
                                    RESULTS, TOTAL_TIME, AUDIO_DURATION,
                                    CHUNK_SIZE, CHUNK_OVERLAP, CHUNK_WORKERS)


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

class FileRecognizerChunks(BaseRecognizer):
    def __init__(self, dejavu):
        super().__init__(dejavu)

    def make_chunks(self, audio, chunk_length, overlap_length):
        chunks = []
        i = 0
        while i < len(audio):
            chunks.append(audio[i:i+chunk_length])
            i += chunk_length - overlap_length
        return chunks
    
    def process_chunk(self, i, chunk):
        t = time()

        chunkNameExport = "/tmp/chunk-{}.mp3".format(i)
        chunk.export(chunkNameExport, format="mp3", bitrate="128k", codec="libmp3lame")
        channels, frame_rate, _, _ = decoder.read(chunkNameExport, self.dejavu.limit)

        fingerprint_times = []
        hashes = set()  # to remove possible duplicated fingerprints we built a set.
        for channel in channels:
            fingerprints, fingerprint_time = self.dejavu.generate_fingerprints(channel, Fs=frame_rate)
            fingerprint_times.append(fingerprint_time)
            hashes |= set(fingerprints)
        os.remove(chunkNameExport)
        chunk_processing_time = time() - t
        
        return {
            'chunk': i,
            'offset_chunk': i * CHUNK_SIZE,
            'chunk_processing_time': chunk_processing_time,
            'hashes': hashes,
            'fingerprint_times': fingerprint_times
        }
    
    def recognize_file(self, filename: str, options) -> Dict[str, any]:        
        # Leemos el fichero y creamos los chunks
        t = time()

        audio = AudioSegment.from_file(filename)
        chunks = self.make_chunks(audio, int(CHUNK_SIZE * 1000), int(CHUNK_OVERLAP * 1000))
        print('chunks: {}'.format(len(chunks)))

        chunk_make_time = time() - t
        print('chunk make time: {}'.format(chunk_make_time))

        # Procesamos los chunks
        t = time()
        results = dict()
        with concurrent.futures.ThreadPoolExecutor(max_workers=CHUNK_WORKERS) as executor:
            future_to_chunk = { executor.submit(self.process_chunk, i, chunk): i for i, chunk in enumerate(chunks) }
            for future in concurrent.futures.as_completed(future_to_chunk):
                i = future_to_chunk[future]
                try:
                    data = future.result()
                except Exception as exc:
                    print('chunk {} generated an exception: {}'.format(i, exc))
                else:
                    # Store the result in the dictionary using chunk index as the key
                    if data:
                        results[i] = data

        chunk_process_time = time() - t
        print('chunk process time: {}'.format(chunk_process_time))

        ## matches, dedup_hashes, query_time = self.dejavu.find_matches(hashes)

        resultsMatches, query_time = self.dejavu.find_matches_chunk(results, options)
        print('query time: {}'.format(query_time))

        print('resultsMatches: {}'.format(len(resultsMatches)))

        final_results = []
        for i, chunk in resultsMatches.items():
            t = time()
            align_results = self.dejavu.align_matches(chunk['matches'], chunk['dedup_hashes'], len(chunk['hashes']))
            align_time = time() - t

            """ data['ofsset_detection'] = data['offset_chunk'] + -data['results']['offset_seconds']
            data['detection_time'] = num_a_tiempo(data['ofsset_detection'])
            data['song_id'] = data['results']['song_id']
            data['avg_counts_hashes_matched'] = data['results']['avg_counts_hashes_matched']
            del data['results'] """

            if align_results and len(align_results) > 0:
                data = align_results[0]

                final_results.append({
                    # "results": align_results, 
                    "ofsset_detection": chunk['offset_chunk'] + -data['offset_seconds'],
                    "detection_time": self.num_a_tiempo(chunk['offset_chunk'] + -data['offset_seconds']),
                    "song_id": data['song_id'],
                    "song_name": data['song_name'],
                    "avg_counts_hashes_matched": data['avg_counts_hashes_matched'],
                    "fingerprint_times": np.sum(chunk['fingerprint_times']), 
                    "query_time": query_time, 
                    "align_time": align_time
                })

        
        print('final_results: {}'.format(len(final_results)))
        print('final_results: {}'.format(final_results))

        ordered_results = self.process_json(final_results, 0)
        return ordered_results


    def recognize(self, filename: str, options: Dict[str, Any] = {}) -> Dict[str, any]:
        return self.recognize_file(filename, options)

    def num_a_tiempo(self, num):
        # Convertir el número a un objeto timedelta
        tiempo = datetime.timedelta(seconds=num)

        # Formatear el tiempo como HH:MM:SS.SSS
        _, resto = divmod(tiempo.seconds, 3600)
        minutos, segundos = divmod(resto, 60)
        milisegundos = tiempo.microseconds // 1000
        return "{:02}:{:02}.{}".format(minutos, segundos, milisegundos)
    
    def process_json(self, json_data, threshold):
        # Filtrar las detecciones con 'avg_counts_hashes_matched' mayor a threshold
        filtered_detections = [detection for detection in json_data if detection["avg_counts_hashes_matched"] > threshold]
        
        # Agrupar las detecciones por 'song_id' y rango de tiempo 'ofsset_detection'
        grouped_detections = {}
        for detection in filtered_detections:
            key = (detection["song_id"], int(detection["ofsset_detection"] / 5))
            if key not in grouped_detections:
                grouped_detections[key] = {"avg_counts_hashes_matched": [], "ofsset_detection": detection["ofsset_detection"]}
            grouped_detections[key]["avg_counts_hashes_matched"].append(detection["avg_counts_hashes_matched"])
        
        # Calcular la media de 'avg_counts_hashes_matched' y quedarnos con el mínimo 'ofsset_detection' en cada grupo
        final_results = []
        for key, value in grouped_detections.items():
            avg_counts_hashes_matched = sum(value["avg_counts_hashes_matched"]) / len(value["avg_counts_hashes_matched"])
            final_results.append({
                "song_id": key[0],
                "ofsset_detection": value["ofsset_detection"],
                "detection_time": self.num_a_tiempo(value["ofsset_detection"]),
                "avg_counts_hashes_matched": avg_counts_hashes_matched
            })
        
        # Ordenar los resultados por 'ofsset_detection'
        final_results = sorted(final_results, key=lambda x: x["ofsset_detection"])
        
        # Crear un diccionario con el resultado final
        result = {"results": final_results}
        
        # Convertir el diccionario a formato JSON y devolverlo
        return result