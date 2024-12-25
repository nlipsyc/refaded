CREATE TABLE songs (
    id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    artist text,
    title text NOT NULL,
    lyrics text NOT NULL
);

CREATE TABLE ngrams (
    id bigint  PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    song_id bigint REFERENCES songs,
    ngram text NOT NULL,
    start_in_song SMALLINT NOT NULL,
    end_in_song SMALLINT NOT NULL
);
