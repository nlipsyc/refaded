from pathlib import Path

import polars as pl
import pytest
from numerizer import numerize  # noqa: F401
from spacy.tokens import Doc

from main import (
    generate_ngrams_from_lyrics,
    ingest_csv,
    is_valid_token,
    ngram_generator,
)


def test_ingest_csv():
    df = ingest_csv(Path("tests/test_data.csv")).collect()  # noqa: F821
    assert isinstance(df, pl.DataFrame)
    assert df.columns == [
        "title",
        "tag",
        "artist",
        "year",
        "views",
        "features",
        "lyrics",
        "id",
        "language_cld3",
        "language_ft",
        "language",
    ]
    assert list(df["artist"]) == [
        "Cam'ron",
        "JAY-Z",
        "Fabolous",
        "Cam'ron",
        "Lil Wayne",
        "Lil Wayne",
        "Clipse",
        "Cam'ron",
        "Cam'ron",
        "Cam'ron",
    ]
    assert list(df["title"]) == [
        "Killa Cam",
        "Can I Live",
        "Forgive Me Father",
        "Down and Out",
        "Fly In",
        "Lollipop Remix",
        "Im Not You",
        "Family Ties",
        "Rockin and Rollin",
        "Lord You Know",
    ]


@pytest.mark.parametrize(
    ["token", "expected"],
    [
        ("Adidas", True),
        ("the", False),
        ("three", True),
        ("3", True),
        (" ", False),
        ("a", False),
        ("Jay", True),
    ],
)
def test_is_valid_token(token: str, expected: bool, create_token):
    assert is_valid_token(create_token(token)) is expected


@pytest.fixture
def lyrics() -> str:
    return (
        "I was going two and a half blocks to the store and saw three hundred flies "
        "and a goat."
    )


@pytest.fixture
def doc(lyrics: str, create_doc) -> Doc:
    return create_doc(lyrics)


# @pytest.mark.parametrize(["token_index", "expected_string"], [(1, "foo")])
# def test_get_ngram_component_from_token(
#     doc: Doc, token_index: int, expected_string: str
# ):
#     assert get_ngram_component_from_token(doc[token_index]) == expected_string


def test_ngram_generator(doc: Doc):
    tokens = [t for t in doc]
    assert len(tokens) == 20

    ngrams = [n for n in ngram_generator(tokens, ngram_length=5)]
    assert len(ngrams) == 16

    assert [token.text for token in ngrams[3]] == ["two", "and", "a", "half", "blocks"]


def test_generate_ngrams_from_lyrics(lyrics):
    ngrams = generate_ngrams_from_lyrics(lyrics)
    # Is set as a constant in main.py
    assert all(len(ngram) == 5 for ngram in ngrams)
    assert [
        [token._.processed_representation for token in ngram] for ngram in ngrams
    ] == [
        ["go", "2.5", "block", "store", "see"],
        ["2.5", "block", "store", "see", "300"],
        ["block", "store", "see", "300", "fly"],
        ["store", "see", "300", "fly", "goat"],
    ]
