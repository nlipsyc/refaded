from pathlib import Path
from typing import Union
from uuid import UUID
import polars as pl
import spacy
from uuid import UUID
from spacy.tokens import Doc, Token

from db import RelationalDB, insert_song, insert_ngrams

# %%
nlp = spacy.load("en_core_web_sm")


def ingest_csv(filename: Path | str = "song_lyrics.csv") -> pl.LazyFrame:
    return pl.scan_csv(filename).filter(pl.col("tag").is_in(["rap", "misc", "rb"]))


def is_valid_token(token: Token) -> bool:
    # Is stop includes numbers. Go over this an decide if it's desired
    token_is_invalid = token.is_stop or token.is_punct or token.is_space
    return not token_is_invalid


def get_ngram_component_from_token(token: Token) -> str:
    """Determine the appropriate string representation of a token."""
    if numerized := token._.numerized != token.text:
        return numerized
    return token.lemma_


def ngram_generator(lyrics: Doc, ngram_length=5) -> list[Token]:
    """Iterate through a song, yielding lists of ngrams until no full ones remain."""

    tokens_in_ngram: list[Token] = []
    for potential_token in lyrics:
        if len(tokens_in_ngram) < ngram_length:
            if is_valid_token(potential_token):
                tokens_in_ngram.append(potential_token)

        # to_yield = Ngram(
        #     clean_text=" ".join(token.lemma_ for token in tokens_in_ngram),
        #     start_idx_unprocessed=tokens_in_ngram[0].idx,
        #     end_idx_unprocessed=tokens_in_ngram[-1].idx+len(tokens_in_ngram[-1].idx),
        # )
        elif len(tokens_in_ngram) < ngram_length:
            raise StopIteration
        else:
            tokens_to_return = tokens_in_ngram
            tokens_in_ngram = []
            yield tokens_to_return


# %%


def generate_ngrams_from_lyrics(lyrics: str, song_id: UUID) -> list[Token]:
    doc: Doc = nlp(lyrics)

    # Here we should transform these lists of tokens into Ngram objects that will
    # match up with the DB table
    return [ngram for ngram in ngram_generator(doc)]


# %%
def _token_to_db(ngram: list[Token], song_id: UUID) -> tuple[str, str, int, int]:
    ngram_text = " ".join([token.text for token in ngram])
    start_index = ngram[0].idx
    end_index = ngram[-1] + len(ngram[-1])

    return (ngram_text, song_id, start_index, end_index)


def generate_song_insert_values(song: dict[str, str]) -> tuple[str]:
    lyrics = song["lyrics"]

    values = (song["artist"], lyrics)
    return values


# %%
# generate_song_insert_values(next(rap[1].iter_rows(named=True)))

# %%


def process_songs():
    db = RelationalDB()
    for i, song in enumerate(rap.iter_rows(named=True)):
        if i >= 5:
            break
        song_values = generate_song_insert_values(song)

        song_id = insert_song(song_values, db)
        insert_ngrams(song["lyrics"], song_id)

        print([c for c in db.cur])
