# chunk_file_recognizer.py
import numpy as np
import librosa
from dejavu.logic.recognizer.file_recognizer import FileRecognizer


class ChunkFileRecognizer:
    def __init__(self, dejavu):
        self.dejavu = dejavu

    def recognize_file(self, file_path):
        # Cargamos el archivo de audio y lo dividimos en fragmentos
        chunks, chunk_times = self.load_and_split_audio(file_path, chunk_duration=10, overlap=5)

        # Procesamos cada fragmento
        results = []
        recognizer = FileRecognizer(self.dejavu)
        for chunk, chunk_time in zip(chunks, chunk_times):
            result = recognizer.recognize(chunk)
            result["chunk_start_time"] = chunk_time[0]
            result["chunk_end_time"] = chunk_time[1]

            # Calcula la ubicación de inicio para cada coincidencia
            for match in result["results"]:
                match_start = chunk_time[0] + match["offset_seconds"]
                match["match_start_time"] = match_start

            results.append(result)

    def load_and_split_audio(self, file_path, chunk_duration, overlap):
        # Cargamos el archivo de audio
        y, sr = librosa.load(file_path)

        # Dividimos en chunks de la duración deseada con la superposición deseada
        chunks, chunk_times = self.split_into_chunks(y, sr, chunk_duration, overlap)

        return chunks, chunk_times

    def split_into_chunks(self, y, sr, chunk_duration, overlap):
        frames_per_chunk = chunk_duration * sr
        frames_overlap = overlap * sr

        # Usamos un hop_length que es menor que el frame_length para tener superposición
        chunks = librosa.util.frame(y, frame_length=int(frames_per_chunk), hop_length=int(frames_overlap))

        # Convertimos cada frame a una lista de chunks de audio
        chunks = chunks.transpose()

        # Crear los tiempos de inicio y fin para cada chunk
        chunk_times = []
        for i in range(chunks.shape[0]):
            start_time = i * overlap  # Tiempo de inicio en segundos
            end_time = start_time + chunk_duration  # Tiempo de finalización en segundos
            chunk_times.append((start_time, end_time))

        return [chunk for chunk in chunks], chunk_times
