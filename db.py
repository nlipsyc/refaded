import string
import psycopg
from psycopg import sql
from spacy.tokens import Token


class RelationalDB:
    def __init__(self, host="localhost", user="postgres", password="pass1234") -> None:
        self._conn_string = f"host={host} user={user} password={password}"
        self.conn = psycopg.connect(self._conn_string)
        self.cur = self.conn.cursor()


def insert_song(values: tuple[str, str, str], db: RelationalDB) -> int:
    query = sql.SQL(
        "INSERT INTO songs (artist, title, lyrics) VALUES (%s, %s, %s) RETURNING id;"
    )
    res = db.cur.execute(query, values)

    db.conn.commit()
    return res.fetchone()[0]


def format_ngram_for_db(ngram: list[Token], song_id: int) -> tuple[str, int, int, int]:
    """Functionally an entity that handles DB serialization.

    Full DDD is overkill for something like this I think.
    """
    ngram_text = " ".join([token._.processed_representation for token in ngram])
    ngram_text = ngram_text.lower()
    ngram_text = ngram_text.translate(str.maketrans("", "", string.punctuation))
    start_index = ngram[0].idx
    end_index = ngram[-1].idx

    return (ngram_text, song_id, start_index, end_index)


def insert_ngrams(ngrams: list[list[Token]], song_id: int, db: RelationalDB):
    # Make sure it's called with this
    # ngrams = generate_ngrams_from_lyrics(lyrics, song_id)
    # Must also run the output through _token_to_db
    payload = [format_ngram_for_db(ngram, song_id) for ngram in ngrams]
    query = sql.SQL(
        "INSERT INTO ngrams (ngram, song_id, start_in_song, end_in_song) VALUES (%s, %s, %s, %s);"
    )
    db.cur.executemany(query, payload)

    db.conn.commit()
