CREATE TABLE songs (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    artist text,
    title text NOT NULL,
    lyrics text NOT NULL
);

CREATE TABLE ngram_contents (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    contents text UNIQUE NOT NULL
);

CREATE TABLE ngrams (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    song_id uuid REFERENCES songs,
    ngram_contents_id uuid REFERENCES ngram_contents,
    start_in_song SMALLINT NOT NULL,
    end_in_song SMALLINT NOT NULL
);