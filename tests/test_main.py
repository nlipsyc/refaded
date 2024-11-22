from pathlib import Path

import polars as pl
import pytest
import spacy
from numerizer import numerize  # noqa: F401
from spacy.tokens import Doc, Token

from main import (
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


def create_doc(text: str) -> Doc:
    nlp = spacy.load("en_core_web_sm")
    return nlp(text)


def create_token(text: str) -> Token:
    return create_doc(text)[0]


@pytest.mark.parametrize(
    ["token", "expected"],
    [
        (create_token("Adidas"), True),
        (create_token("the"), False),
        (create_token("three"), True),
        (create_token("3"), True),
        (create_token(" "), False),
        (create_token("a"), False),
        (create_token("Jay"), True),
    ],
)
def test_is_valid_token(token: Token, expected: bool):
    assert is_valid_token(token) is expected


@pytest.fixture
def doc() -> Doc:
    text = (
        "I was going two and a half blocks to the store and saw three hundred flies "
        "and a goat."
    )
    return create_doc(text)


# @pytest.mark.parametrize(["token_index", "expected_string"], [(1, "foo")])
# def test_get_ngram_component_from_token(
#     doc: Doc, token_index: int, expected_string: str
# ):
#     assert get_ngram_component_from_token(doc[token_index]) == expected_string


def test_ngram_generator(doc: Doc):
    tokens = [t for t in doc]
    assert len(tokens) == 20

    ngrams = [n for n in ngram_generator(tokens, ngram_length=5)]
    assert len(ngrams) == 15

    assert ngrams[3] == ["two", "and", "a", "half", "blocks"]


def test_generate_ngrams_from_lyrics():
    ...
    # """TODO"""
