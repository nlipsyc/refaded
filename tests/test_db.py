import uuid
import pytest

from db import RelationalDB, insert_ngrams, insert_song
from main import generate_ngrams_from_lyrics


@pytest.fixture(autouse=True)
def config_db(postgresql):
    """Create tables."""
    with open("bootstrap.sql", "r") as bootstrap:
        cur = postgresql.cursor()
        cur.execute("\n".join(line for line in bootstrap))
        postgresql.commit()
        cur.close()


@pytest.fixture
def db(postgresql):
    db = RelationalDB()
    db.conn = postgresql
    db.cur = postgresql.cursor()
    return db


def test_test_db(postgresql):
    """If this is failing, specify postgres exec in pytest.ini.

    e.g.
    [pytest]
    postgresql_exec = /usr/lib/postgresql/14/bin/pg_ctl
    """
    cur = postgresql.cursor()
    cur.execute("SELECT * FROM songs;")
    # Raises an exception if doesn't exist
    assert cur.fetchone() is None


def test_insert_song(postgresql, db):
    expected_artist = "A Tribe Called Quest"
    expected_lyrics = (
        "Yo, Phife, you remember that routine \n That we used to make spiffy like"
        "Mr. Clean?"
    )
    expected_ngrams = [
        ["Yo", "Phife", "remember", "routine", "spiffy"],
        ["Phife", "remember", "routine", "spiffy", "like"],
        ["remember", "routine", "spiffy", "like", "Mr."],
        ["routine", "spiffy", "like", "Mr.", "Clean"],
    ]

    song_id = insert_song((expected_artist, expected_lyrics), db)
    assert isinstance(song_id, uuid.UUID)

    cur = postgresql.cursor()
    cur.execute("SELECT artist, lyrics FROM songs WHERE id = %s;", (song_id,))
    artist, lyrics = cur.fetchone()
    assert artist == expected_artist
    assert lyrics == expected_lyrics


@pytest.fixture()
def song_in_db(db: RelationalDB) -> uuid.UUID:
    artist = "A Tribe Called Quest"
    lyrics = (
        "Yo, Phife, you remember that routine \n That we used to make spiffy like "
        "Mr. Clean?"
    )
    song_id = insert_song((artist, lyrics), db)
    return song_id


def test_insert_ngrams(postgresql, db: RelationalDB, song_in_db: uuid.UUID):
    expected_ngrams = [
        ["Yo", "Phife", "remember", "routine", "spiffy"],
        ["Phife", "remember", "routine", "spiffy", "like"],
        ["remember", "routine", "spiffy", "like", "Mr."],
        ["routine", "spiffy", "like", "Mr.", "Clean"],
    ]
    lyrics = (
        "Yo, Phife, you remember that routine \n That we used to make spiffy like "
        "Mr. Clean?"
    )
    expected_ngrams = generate_ngrams_from_lyrics(lyrics)

    # tokenized_ngrams = [
    #     [create_token(word) for word in ngram] for ngram in expected_ngrams
    # ]
    insert_ngrams(expected_ngrams, song_in_db, db)

    cur = postgresql.cursor()
    cur.execute("SELECT * FROM ngrams WHERE song_id = %s;", (song_in_db,))
    ngrams = cur.fetchall()
    assert len(ngrams) == 4
    _, song_id, ngram_text, start_index, end_index = ngrams[3]
    assert song_id == song_in_db
    assert ngram_text == "routine spiffy like mr clean"
    assert start_index == 29
    assert end_index == 76
