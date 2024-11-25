from pathlib import Path
from typing import Any, Generator
from uuid import UUID
import polars as pl
import spacy
from spacy.tokens import Doc, Token
from numerizer import spacy_numerize

from db import RelationalDB, insert_song, insert_ngrams

NGRAM_LENGTH = 5
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


def set_spacy_attrs():
    Token.set_extension("processed_representation", default=None)


set_spacy_attrs()


def ingest_csv(filename: Path | str = "song_lyrics.csv") -> pl.LazyFrame:
    return pl.scan_csv(filename).filter(pl.col("tag").is_in(["rap", "misc", "rb"]))


def is_valid_token(token: Token) -> bool:
    # Is stop includes numbers. Go over this an decide if it's desired
    token_is_invalid = token.is_stop or token.is_punct or token.is_space
    return not token_is_invalid


def process_ngram_token(token: Token) -> Token:
    """Determine the appropriate string representation of a token."""
    # TODO Mr. Mr Mister are all different currently. Fix?
    # Mr., Mr, mr., and mr are all the same now
    if token._.numerized != token.text:
        token._.processed_representation = token._.numerized
    else:
        token._.processed_representation = token.lemma_
    return token


def ngram_generator(
    lyrics: list[Token], ngram_length=NGRAM_LENGTH
) -> Generator[list[Token], Any, None]:
    """Iterate through a song, yielding lists of ngrams until no full ones remain."""

    last_ngramable_token = len(lyrics) - ngram_length
    for i, _ in enumerate(lyrics):
        if i <= last_ngramable_token:
            yield lyrics[i : i + ngram_length]
        else:
            return


def generate_ngrams_from_lyrics(lyrics: str) -> list[list[Token]]:
    """Given a song lyric, create all possible ngrams from it."""
    doc: Doc = nlp(lyrics)
    numerized = spacy_numerize(doc, retokenize=True)
    valid_tokens = [
        process_ngram_token(token) for token in numerized if is_valid_token(token)
    ]

    ngrams = ngram_generator(valid_tokens)
    return [ngram for ngram in ngrams]


def process_song(song: dict):
    # Move this up a level if we don't want to commit so frequently?
    db = RelationalDB()
    lyrics = song["lyrics"]
    # TODO Make sure these are all in consistent title case
    song_id = insert_song((song["artist"], lyrics), db)
    ngrams = generate_ngrams_from_lyrics(lyrics)
    insert_ngrams(ngrams, song_id, db)


if __name__ == "__main__":
    data = ingest_csv()
    # TODO Come back to this and batch it with slices
    # https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.LazyFrame.slice.html

    for row in data.collect.iter_rows(named=True):
        process_song(row)
