from ..main import ingest_csv
import polars as pl
from pathlib import Path


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
