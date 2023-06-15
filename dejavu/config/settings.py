# Dejavu
import os

# RESPUESTA JSON DE DEJAVU
SONG_ID = "song_id"
SONG_NAME = 'song_name'
RESULTS = 'results'

HASHES_MATCHED = 'hashes_matched_in_input'

# Huellas digitalizadas en la base de datos.
FINGERPRINTED_HASHES = 'fingerprinted_hashes_in_db'
# Porcentaje referente a las huellas coincidentes vs las huellas digitalizadas en la base de datos.
FINGERPRINTED_CONFIDENCE = 'fingerprinted_confidence'

# Huellas generadas desde la entrada.
INPUT_HASHES = 'input_total_hashes'
# Porcentaje referente a las huellas coincidentes vs las huellas de la entrada.
INPUT_CONFIDENCE = 'input_confidence'

TOTAL_TIME = 'total_time'
FINGERPRINT_TIME = 'fingerprint_time'
QUERY_TIME = 'query_time'
ALIGN_TIME = 'align_time'
OFFSET = 'offset'
OFFSET_SECS = 'offset_seconds'

# INSTANCIAS DE CLASES DE BASE DE DATOS:
DATABASES = {
    'mysql': ("dejavu.database_handler.mysql_database", "MySQLDatabase"),
}
# 'postgres': ("dejavu.database_handler.postgres_database", "PostgreSQLDatabase")

# TABLA SONGS
SONGS_TABLENAME = "songs"

# CAMPOS DE SONGS
FIELD_SONG_ID = 'song_id'
FIELD_SONGNAME = 'song_name'
FIELD_FINGERPRINTED = "fingerprinted"
FIELD_FILE_SHA1 = 'file_sha1'
FIELD_TOTAL_HASHES = 'total_hashes'
FIELD_AUDIO_DURATION = 'duration'

# TABLA FINGERPRINTS
FINGERPRINTS_TABLENAME = "fingerprints"

# CAMPOS DE FINGERPRINTS
FIELD_HASH = 'hash'
FIELD_OFFSET = 'offset'

# CONFIGURACIÓN DE FINGERPRINTS:
# Esto se utiliza como parámetro de conectividad para la función scipy.generate_binary_structure. Este parámetro
# cambia la máscara de morfología al buscar los picos máximos en la matriz del espectrograma.
# Los valores posibles son: [1, 2]
# Donde 1 establece una morfología de diamante, lo que implica que los elementos diagonales no se consideran vecinos (este
# es el valor utilizado en el código original de dejavu).
# Y 2 establece una máscara cuadrada, es decir, todos los elementos son considerados vecinos.
CONNECTIVITY_MASK = int(os.getenv('DJV_CONNECTIVITY_MASK', 2))

# Tasa de muestreo, relacionada con las condiciones de Nyquist, que afecta
# el rango de frecuencias que podemos detectar.
DEFAULT_FS = int(os.getenv('DJV_DEFAULT_FS', 44100))

# Tamaño de la ventana FFT, afecta la granularidad de la frecuencia
DEFAULT_WINDOW_SIZE = 4096

# Relación por la cual cada ventana secuencial se superpone con la última y la
# próxima ventana. Una superposición mayor permitirá una mayor granularidad en el desplazamiento
# correspondiente, pero potencialmente más huellas digitales.
DEFAULT_OVERLAP_RATIO = 0.5

# Grado en el que una huella digital puede emparejarse con sus vecinos. Valores más altos darán lugar a
# más huellas digitales, pero potencialmente mejor precisión.
DEFAULT_FAN_VALUE = int(os.getenv('DJV_DEFAULT_FAN_VALUE', 15))  # 15 era el valor original.

# Amplitud mínima en el espectrograma para ser considerado un pico.
# Esto puede aumentarse para reducir el número de huellas digitales, pero puede afectar negativamente
# la precisión.
DEFAULT_AMP_MIN = 10

# Número de celdas alrededor de un pico de amplitud en el espectrograma para
# que Dejavu lo considere un pico espectral. Valores más altos significan menos
# huellas digitales y emparejamiento más rápido, pero pueden afectar potencialmente la precisión.
PEAK_NEIGHBORHOOD_SIZE = 10  # 20 era el valor original.

# Umbrales de cuán cerca o lejos pueden estar las huellas digitales en el tiempo para
# ser emparejadas como una huella digital. Si tu máximo es demasiado bajo, los valores más altos de
# DEFAULT_FAN_VALUE pueden no funcionar como se esperaba.
MIN_HASH_TIME_DELTA = 0
MAX_HASH_TIME_DELTA = 200

# Si es True, ordenará los picos temporalmente para la digitalización de huellas;
# no ordenar reducirá la cantidad de huellas digitales, pero potencialmente
# afectará el rendimiento.
PEAK_SORT = True

# Número de bits para tomar del frente del hash SHA1 en el
# cálculo de la huella digital. Cuantos más tomes, más almacenamiento en memoria,
# con potencialmente menos colisiones de coincidencias.
FINGERPRINT_REDUCTION = int(os.getenv('DJV_FINGERPRINT_REDUCTION', 20))

# Número de resultados que se devuelven para el reconocimiento de archivos
TOPN = int(os.getenv('DJV_TOPN', 2))
