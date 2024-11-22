from pathlib import Path
from uuid import UUID
import polars as pl
import spacy
from uuid import UUID
from spacy.tokens import Doc, Token
from numerizer import spacy_numerize

from db import RelationalDB, insert_song, insert_ngrams

# Mabye stick this in another file?
NON_STOP_WORDS = {
    "zero",
    "one",
    "two",
    "three",
    "four",
    "five",
    "six",
    "seven",
    "eight",
    "nine",
    "ten",
    "eleven",
    "twelve",
    "thirteen",
    "fourteen",
    "fifteen",
    "sixteen",
    "seventeen",
    "eighteen",
    "nineteen",
    "twenty",
}
nlp = spacy.load("en_core_web_sm")
nlp.Defaults.stop_words -= NON_STOP_WORDS


def ingest_csv(filename: Path | str = "song_lyrics.csv") -> pl.LazyFrame:
    return pl.scan_csv(filename).filter(pl.col("tag").is_in(["rap", "misc", "rb"]))


def is_valid_token(token: Token) -> bool:
    # Is stop includes numbers. Go over this an decide if it's desired
    token_is_invalid = token.is_stop or token.is_punct or token.is_space
    return not token_is_invalid


def process_ngram_token(token: Token) -> str:
    """Determine the appropriate string representation of a token."""
    if numerized := token._.numerized != token.text:
        return numerized
    return token.lemma_


def ngram_generator(lyrics: list[Token], ngram_length=5) -> list[str]:
    """Iterate through a song, yielding lists of ngrams until no full ones remain."""

    last_ngramable_token = len(lyrics) - ngram_length
    for i, _ in enumerate(lyrics):
        if i < last_ngramable_token:
            yield [token.text for token in lyrics[i : i + ngram_length]]


def generate_ngrams_from_lyrics(lyrics: str, song_id: UUID) -> list[Token]:
    doc: Doc = nlp(lyrics)
    numerized = spacy_numerize(doc, retokenize=True)
    valid_tokens = [
        process_ngram_token(token) for token in numerized if is_valid_token(token)
    ]

    # Here we should transform these lists of tokens into Ngram objects that will
    # match up with the DB table
    ngrams = ngram_generator(valid_tokens)
    return [ngram for ngram in ngrams]


def generate_song_insert_values(song: dict[str, str]) -> tuple[str]:
    lyrics = song["lyrics"]

    values = (song["artist"], lyrics)
    return values


def process_songs():
    db = RelationalDB()
    for i, song in enumerate(rap.iter_rows(named=True)):
        if i >= 5:
            break
        song_values = generate_song_insert_values(song)

        song_id = insert_song(song_values, db)
        insert_ngrams(song["lyrics"], song_id)

        print([c for c in db.cur])


def process_song(song: dict):
    # Move this up a level if we don't want to commit so frequently?
    db = RelationalDB()
    lyrics = song["lyrics"]
    song_id = insert_song((song["artist"], lyrics), db)
    ngrams = generate_ngrams_from_lyrics(lyrics)
    insert_ngrams(ngrams, song_id)
    lyrics = nlp(song["lyrics"])


if __name__ == "__main__":
    data = ingest_csv()
    # Come back to this and batch it with slices
    # https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.LazyFrame.slice.html

    for row in data.collect.iter_rows(named=True):
        process_song(row)
