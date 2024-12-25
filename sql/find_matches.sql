SELECT
    instances,
    ngram_ids,
    artist_ids,
    song_ids,
    ngram
FROM
    (
        SELECT
            count(*) AS instances,
            array_agg(ngrams.id) AS ngram_ids,
            array_agg(DISTINCT (songs.artist)) AS artist_ids,
            array_agg(DISTINCT (ngrams.song_id)) AS song_ids,
            ngram
        FROM
            ngrams
            JOIN songs ON ngrams.song_id = songs.id
        GROUP BY
            ngram
    ) ngrams_by_song (instances, ngram_ids, artist_ids, song_ids, ngram)
ORDER BY
    array_length(artist_ids, 1) DESC;