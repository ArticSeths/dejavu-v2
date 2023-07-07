import abc
from typing import Dict, List, Tuple

from dejavu.base_classes.base_database import BaseDatabase
from dejavu.config.settings import (FINGERPRINTS_TABLENAME, FIELD_SONG_ID,
                                    FIELD_HASH, FIELD_OFFSET)

                                    


class CommonDatabase(BaseDatabase, metaclass=abc.ABCMeta):
    # Since several methods across different databases are actually just the same
    # I've built this class with the idea to reuse that logic instead of copy pasting
    # over and over the same code.

    def __init__(self):
        super().__init__()

    def before_fork(self) -> None:
        """
        Called before the database instance is given to the new process
        """
        pass

    def after_fork(self) -> None:
        """
        Called after the database instance has been given to the new process

        This will be called in the new process.
        """
        pass

    def setup(self) -> None:
        """
        Called on creation or shortly afterwards.
        """
        # with self.cursor() as cur:
            ## TODO: OJO no jesus
            # cur.execute(self.CREATE_SONGS_TABLE)
            # cur.execute(self.CREATE_FINGERPRINTS_TABLE)
            # cur.execute(self.DELETE_UNFINGERPRINTED)
        pass

    def empty(self) -> None:
        """
        Called when the database should be cleared of all data.
        """
        with self.cursor() as cur:
            cur.execute(self.DROP_FINGERPRINTS)
            cur.execute(self.DROP_SONGS)

        self.setup()

    def delete_unfingerprinted_songs(self) -> None:
        """
        Called to remove any song entries that do not have any fingerprints
        associated with them.
        """
        with self.cursor() as cur:
            cur.execute(self.DELETE_UNFINGERPRINTED)

    def get_num_songs(self) -> int:
        """
        Returns the song's count stored.

        :return: the amount of songs in the database.
        """
        with self.cursor(buffered=True) as cur:
            cur.execute(self.SELECT_UNIQUE_SONG_IDS)
            count = cur.fetchone()[0] if cur.rowcount != 0 else 0

        return count

    def get_num_fingerprints(self) -> int:
        """
        Returns the fingerprints' count stored.

        :return: the number of fingerprints in the database.
        """
        with self.cursor(buffered=True) as cur:
            cur.execute(self.SELECT_NUM_FINGERPRINTS)
            count = cur.fetchone()[0] if cur.rowcount != 0 else 0

        return count

    def set_song_fingerprinted(self, song_id):
        """
        Sets a specific song as having all fingerprints in the database.

        :param song_id: song identifier.
        """
        with self.cursor() as cur:
            cur.execute(self.UPDATE_SONG_FINGERPRINTED, (song_id,))

    def get_songs(self) -> List[Dict[str, str]]:
        """
        Returns all fully fingerprinted songs in the database

        :return: a dictionary with the songs info.
        """
        with self.cursor(dictionary=True) as cur:
            cur.execute(self.SELECT_SONGS)
            return list(cur)

    def get_song_by_id(self, song_id: int) -> Dict[str, str]:
        """
        Brings the song info from the database.

        :param song_id: song identifier.
        :return: a song by its identifier. Result must be a Dictionary.
        """
        with self.cursor(dictionary=True) as cur:
            cur.execute(self.SELECT_SONG, (song_id,))
            return cur.fetchone()

    def insert(self, fingerprint: str, song_id: int, offset: int):
        """
        Inserts a single fingerprint into the database.

        :param fingerprint: Part of a sha1 hash, in hexadecimal format
        :param song_id: Song identifier this fingerprint is off
        :param offset: The offset this fingerprint is from.
        """
        with self.cursor() as cur:
            cur.execute(self.INSERT_FINGERPRINT, (fingerprint, song_id, offset))

    @abc.abstractmethod
    def insert_song(self, song_name: str, file_hash: str, total_hashes: int, audio_duration: int) -> int:
        """
        Inserts a song name into the database, returns the new
        identifier of the song.

        :param song_name: The name of the song.
        :param file_hash: Hash from the fingerprinted file.
        :param total_hashes: amount of hashes to be inserted on fingerprint table.
        :param audio_duration: duration of the audio file in milliseconds.
        :return: the inserted id.
        """
        pass

    def query(self, fingerprint: str = None) -> List[Tuple]:
        """
        Returns all matching fingerprint entries associated with
        the given hash as parameter, if None is passed it returns all entries.

        :param fingerprint: part of a sha1 hash, in hexadecimal format
        :return: a list of fingerprint records stored in the db.
        """
        with self.cursor() as cur:
            if fingerprint:
                cur.execute(self.SELECT, (fingerprint,))
            else:  # select all if no key
                cur.execute(self.SELECT_ALL)
            return list(cur)

    def get_iterable_kv_pairs(self) -> List[Tuple]:
        """
        Returns all fingerprints in the database.

        :return: a list containing all fingerprints stored in the db.
        """
        return self.query(None)

    def insert_hashes(self, song_id: int, hashes: List[Tuple[str, int]], batch_size: int = 1000) -> None:
        """
        Insert a multitude of fingerprints.

        :param song_id: Song identifier the fingerprints belong to
        :param hashes: A sequence of tuples in the format (hash, offset)
            - hash: Part of a sha1 hash, in hexadecimal format
            - offset: Offset this hash was created from/at.
        :param batch_size: insert batches.
        """
        values = [(song_id, hsh, int(offset)) for hsh, offset in hashes]

        with self.cursor() as cur:
            for index in range(0, len(hashes), batch_size):
                batch_values = values[index: index + batch_size]
                batch_values_query = ','.join(['(%s, UNHEX(%s), %s)'] * len(batch_values))
                query = f"""
                    INSERT IGNORE INTO `{FINGERPRINTS_TABLENAME}` (
                            `{FIELD_SONG_ID}`
                        ,   `{FIELD_HASH}`
                        ,   `{FIELD_OFFSET}`)
                    VALUES {batch_values_query};
                """
                cur.execute(query, sum(batch_values, ()))
        # METODO ANTIGUO
        """ with self.cursor() as cur:
            for index in range(0, len(hashes), batch_size):
                cur.executemany(self.INSERT_FINGERPRINT, values[index: index + batch_size]) """

    def return_matches(self, hashes: List[Tuple[str, int]]) -> Tuple[List[Tuple[int, int]], Dict[int, int]]:
        """
        Searches the database for pairs of (hash, offset) values.
        """
        # Create a dictionary of hash => offset pairs for later lookups
        mapper = {}
        for hsh, offset in hashes:
            if hsh.upper() not in mapper:
                mapper[hsh.upper()] = [offset]
            else:
                mapper[hsh.upper()].append(offset)
        
        # in order to count each hash only once per db offset we use the dic below
        dedup_hashes = {}
        results = []
        with self.cursor() as cur:
            # Create our IN part of the query
            query = self.SELECT_MULTIPLE % ', '.join([self.IN_MATCH] * len(mapper))

            cur.execute(query, list(mapper.keys()))

            for hsh, sid, offset in cur:
                dedup_hashes[sid] = dedup_hashes.get(sid, 0) + 1

                # we now evaluate all offset for each hash matched
                for song_sampled_offset in mapper[hsh]:
                    results.append((sid, offset - song_sampled_offset))

        return results, dedup_hashes


    def return_matches_OLD(self, hashes: List[Tuple[str, int]],
                       batch_size: int = 1000) -> Tuple[List[Tuple[int, int]], Dict[int, int]]:
        """
        Searches the database for pairs of (hash, offset) values.

        :param hashes: A sequence of tuples in the format (hash, offset)
            - hash: Part of a sha1 hash, in hexadecimal format
            - offset: Offset this hash was created from/at.
        :param batch_size: number of query's batches.
        :return: a list of (sid, offset_difference) tuples and a
        dictionary with the amount of hashes matched (not considering
        duplicated hashes) in each song.
            - song id: Song identifier
            - offset_difference: (database_offset - sampled_offset)
        """
        # Create a dictionary of hash => offset pairs for later lookups
        mapper = {}
        for hsh, offset in hashes:
            if hsh.upper() in mapper.keys():
                mapper[hsh.upper()].append(offset)
            else:
                mapper[hsh.upper()] = [offset]

        values = list(mapper.keys())

        # in order to count each hash only once per db offset we use the dic below
        dedup_hashes = {}

        results = []
        with self.cursor() as cur:
            for index in range(0, len(values), batch_size):
                # Create our IN part of the query
                query = self.SELECT_MULTIPLE % ', '.join([self.IN_MATCH] * len(values[index: index + batch_size]))

                cur.execute(query, values[index: index + batch_size])

                for hsh, sid, offset in cur:
                    if sid not in dedup_hashes.keys():
                        dedup_hashes[sid] = 1
                    else:
                        dedup_hashes[sid] += 1
                    #  we now evaluate all offset for each  hash matched
                    for song_sampled_offset in mapper[hsh]:
                        results.append((sid, offset - song_sampled_offset))

            return results, dedup_hashes

    def delete_songs_by_id(self, song_ids: List[int], batch_size: int = 1000) -> None:
        """
        Given a list of song ids it deletes all songs specified and their corresponding fingerprints.

        :param song_ids: song ids to be deleted from the database.
        :param batch_size: number of query's batches.
        """
        with self.cursor() as cur:
            for index in range(0, len(song_ids), batch_size):
                # Create our IN part of the query
                query = self.DELETE_SONGS % ', '.join(['%s'] * len(song_ids[index: index + batch_size]))

                cur.execute(query, song_ids[index: index + batch_size])

    def return_matches_chunk(self, hashes_group, options) -> Tuple[List[Tuple[int, int]], Dict[int, int]]:
        batch_size = 15000
        """
        Searches the database for pairs of (hash, offset) values.
        """
        mapperFind = {}
        for i, hashes in hashes_group.items():
            mapper = {}
            for hsh, offset in hashes['hashes']:
                if hsh.upper() not in mapperFind:
                    mapperFind[hsh.upper()] = [offset]
                else:
                    mapperFind[hsh.upper()].append(offset)
                
                if hsh.upper() not in mapper:
                    mapper[hsh.upper()] = [offset]
                else:
                    mapper[hsh.upper()].append(offset)
            hashes['mapper'] = mapper
            hashes['hashList'] = list(mapper.keys())
        
        # for debug only: cut the number of hashes to be searched to 10
        # mapperFind = dict(list(mapperFind.items())[:10])

        values = list(mapperFind.keys())

        ddbbResults = []
        with self.cursor() as cur:
            rangeSize = range(0, len(values), batch_size)
            print('rangeSize: {}'.format(len(rangeSize)))

            for index in rangeSize:
                # PRINT DOTS EACH LOOP
                print('.', end='', flush=True)

                # Create our IN part of the query
                song_filter = options.get('song_filter')
                if song_filter and len(song_filter) > 0:
                    song_filter_placeholders = ', '.join(['%s'] * len(song_filter))
                    hash_placeholders = ', '.join([self.IN_MATCH] * len(values[index: index + batch_size]))

                    query = self.SELECT_MULTIPLE_FILTER_SONGS % (song_filter_placeholders, hash_placeholders)
                    cur.execute(query, song_filter + values[index: index + batch_size])
                else:
                    query = self.SELECT_MULTIPLE % ', '.join([self.IN_MATCH] * len(values[index: index + batch_size]))
                    cur.execute(query, values[index: index + batch_size])

                print(':', end='', flush=True)

                for hsh, sid, offset in cur:
                    ddbbResults.append((hsh, sid, offset))

        print('ddbbResults: {}'.format(len(ddbbResults)))

        for i, hashes in hashes_group.items():
            print('processing {} of {}'.format(i, len(hashes_group)))
            dedup_hashes = {}
            results = []
            hashSet = set(hashes['hashList'])
            filtered_ddbbResults = [(hsh, sid, offset) for hsh, sid, offset in ddbbResults if hsh in hashSet]
            print('filtered_ddbbResults: {}'.format(len(filtered_ddbbResults)))
            for hsh, sid, offset in filtered_ddbbResults:
                dedup_hashes[sid] = dedup_hashes.get(sid, 0) + 1
                offsetSet = set([offset - song_sampled_offset for song_sampled_offset in hashes['mapper'][hsh]])
                results.extend([(sid, offset) for offset in offsetSet])

            hashes['matches'] = results
            hashes['dedup_hashes'] = dedup_hashes


        return hashes_group