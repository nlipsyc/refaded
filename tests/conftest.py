from typing import Callable
import spacy
from spacy.tokens import Doc, Token
import pytest


@pytest.fixture()
def create_doc() -> Callable[..., Doc]:
    def inner(text: str) -> Doc:
        nlp = spacy.load("en_core_web_sm")
        return nlp(text)

    return inner


@pytest.fixture()
def create_token(create_doc) -> Callable[..., Token]:
    def inner(text: str) -> Token:
        return create_doc(text)[0]

    return inner
