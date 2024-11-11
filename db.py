import psycopg
from psycopg import sql

from uuid import UUID


class RelationalDB:
    def __init__(self, host="localhost", user="postgres", password="pass1234") -> None:
        self._conn_string = f"host={host} user={user} password={password}"
        self.conn = psycopg.connect(self._conn_string)
        self.cur = self.conn.cursor()


def insert_song(values: tuple[str], db: RelationalDB) -> UUID:
    query = sql.SQL("INSERT INTO songs (artist, lyrics) VALUES (%s, %s) RETURNING id;")
    res = db.cur.execute(query, values)

    db.conn.commit()
    return res.fetchone()[0]


def insert_ngrams(ngrams: tuple[str, str, int, int], db: RelationalDB):
    # Make sure it's called with this
    # ngrams = generate_ngrams_from_lyrics(lyrics, song_id)
    # Must also run the output through _token_to_db
    query = sql.SQL(
        "INSERT INTO ngrams (ngram, song_id, start_in_song, end_in_song) VALUES (%s, %s, %s, %s);"
    )
    res = db.cur.executemany(query, ngrams)

    db.conn.commit()
    # Does this return anything?
    return res.fetchone()[0]
