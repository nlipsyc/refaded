import psycopg
from psycopg import sql
from spacy.tokens import Token

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


def format_ngram_for_db(ngram: list[Token], song_id: UUID) -> tuple[str, str, int, int]:
    """Functionally an entity that handles DB serialization.

    Full DDD if overkill for something like this I think.
    """
    ngram_text = " ".join([token.text for token in ngram])
    start_index = ngram[0].idx
    end_index = ngram[-1] + len(ngram[-1])

    return (ngram_text, song_id, start_index, end_index)


def insert_ngrams(ngram: list[Token], song_id: UUID, db: RelationalDB):
    # Make sure it's called with this
    # ngrams = generate_ngrams_from_lyrics(lyrics, song_id)
    # Must also run the output through _token_to_db
    payload = format_ngram_for_db(ngram, song_id)
    query = sql.SQL(
        "INSERT INTO ngrams (ngram, song_id, start_in_song, end_in_song) VALUES (%s, %s, %s, %s);"
    )
    res = db.cur.executemany(query, payload)

    db.conn.commit()
    # Does this return anything?
    return res.fetchone()[0]
