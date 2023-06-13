# chunk_file_recognizer.py
import numpy as np
import librosa
from dejavu.logic.recognizer.file_recognizer import FileRecognizer


class ChunkFileRecognizer:
    def __init__(self, dejavu):
        self.dejavu = dejavu

    def recognize_file(self, file_path):
        # Cargamos el archivo de audio y lo dividimos en fragmentos
        chunks = self.load_and_split_audio(file_path, chunk_duration=10, overlap=5)

        # Procesamos cada fragmento
        results = []
        recognizer = FileRecognizer(self.dejavu)
        for chunk in chunks:
            result = recognizer.recognize(chunk)
            results.append(result)

        return results

    def load_and_split_audio(self, file_path, chunk_duration, overlap):
        # Cargamos el archivo de audio
        y, sr = librosa.load(file_path)

        # Dividimos en chunks de la duración deseada con la superposición deseada
        chunks = self.split_into_chunks(y, sr, chunk_duration, overlap)

        return chunks

    def split_into_chunks(self, y, sr, chunk_duration, overlap):
        frames_per_chunk = chunk_duration * sr
        frames_overlap = overlap * sr

        # Usamos un hop_length que es menor que el frame_length para tener superposición
        chunks = librosa.util.frame(y, frame_length=int(frames_per_chunk), hop_length=int(frames_overlap))

        # Convertimos cada frame a una lista de chunks de audio
        chunks = chunks.transpose()

        return [chunk for chunk in chunks]
