import argparse
import logging
import time
from concurrent import futures
from pathlib import Path
from typing import Any, Generator

import polars as pl
import spacy
from numerizer import spacy_numerize
from spacy.tokens import Doc, Token

from db import RelationalDB, insert_ngrams, insert_song

log = logging.getLogger(__name__)

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
LANGUAGES_TO_SKIP = {"az", "ru", "tr"}
nlp = spacy.load("en_core_web_sm")
nlp.Defaults.stop_words -= NON_STOP_WORDS


def set_spacy_attrs():
    Token.set_extension("processed_representation", default=None, force=True)


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


def _song_is_valid(song: dict) -> bool:
    """Check if song should be skipped.

    These could all be done on the DF filtering, but they were discovered during
    processing. They are added in here to avoid having to restart the processing as
    each one is found.
    """
    if len(song["lyrics"]) > 30_000:  # smallint index in pg limits this to ~4,500 words
        log.warning(f"Skipping song {song['title']}. Song length suggests it is spam.")
        return False
    elif not song["title"]:
        log.warning(f"Skipping song by artist {song['artist']}. Song has not title.")
        return False
    return True


def _replace_dotless_i(lyrics: str) -> str:
    """Replace Turkish dotless i (ı) with Latin i.

    For some reason this showed up a bunch in the dataset and confused numerizer
    """
    return lyrics.replace("ı", "i")


def process_song(song: dict) -> None:
    db = RelationalDB()
    if not _song_is_valid(song):
        return

    lyrics = song["lyrics"]
    lyrics = _replace_dotless_i(lyrics)

    # TODO Make sure these are all in consistent title case
    song_id = insert_song((song["artist"], song["title"], lyrics), db)
    try:
        ngrams = generate_ngrams_from_lyrics(lyrics)
    except Exception:
        if (
            song["language_cld3"] in LANGUAGES_TO_SKIP
            or song["language_ft"] in LANGUAGES_TO_SKIP
        ):
            # These rarely cause exceptions and we want to include them when they don't.
            log.warning(
                f"Skipping song {song['title']}. Song is in an unsupported language."
            )
            return
        else:
            print(
                f"Ngram processing failed on song {song['title']} by {song['artist']}."
            )
            raise
    insert_ngrams(ngrams, song_id, db)


if __name__ == "__main__":
    logging.basicConfig(filename="refaded_processing.log", level=logging.INFO)

    start_time = time.time()
    parser = argparse.ArgumentParser(
        prog="Refaded", description="Refaded data processing script"
    )
    parser.add_argument("-c", "--cursor", type=int)
    parser.add_argument("-l", "--limit", type=int)
    parser.add_argument("-s", "--step", type=int, default=1_000)

    args = parser.parse_args()

    start_data = time.time()
    data = ingest_csv()
    print(f"CSV ingestion took {time.time()-start_data} seconds")

    cursor = args.cursor
    limit = args.limit
    step = args.step

    while cursor < limit:
        start_chunk = time.time()
        chunk = data.slice(cursor, min(step, limit - cursor))
        print(f"Chunk creation took {time.time()-start_chunk} seconds")

        collect_start = time.time()

        row_counter = 0
        with futures.ProcessPoolExecutor(max_workers=12) as executor:
            for row in executor.map(
                process_song, chunk.collect().iter_rows(named=True)
            ):
                print(f"Processing song {cursor+row_counter}")
                row_counter += 1
        print(
            f"Time elapsed: {round(time.time() - start_time)} seconds. Rows processed: {row_counter}"
        )
        cursor += step
