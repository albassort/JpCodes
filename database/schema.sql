CREATE SCHEMA IF NOT EXISTS training_data;

CREATE TABLE IF NOT EXISTS training_data.SubtitleData (
    id serial PRIMARY KEY,
    data text NOT NULL,
    path text NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS training_data.AozoraBooks (
    id serial PRIMARY KEY,
    aozoraName text NOT NULL,
    aozoraId decimal NOT NULL UNIQUE,
    difficulty decimal NOT NULL,
    data text NOT NULL,
    year int NOT NULL
);

CREATE TABLE IF NOT EXISTS training_data.CharacterCounts (
    id serial PRIMARY KEY,
    char numeric,
    count decimal NOT NULL,
    isKanji boolean NOT NULL,
    -- extendable
    aozoraId int REFERENCES training_data.AozoraBooks (id),
    subtitleId int REFERENCES training_data.SubtitleData (id),
    CONSTRAINT depup_prevention_aozora_1 UNIQUE (aozoraId, char),
    CONSTRAINT depup_prevention_subtitle_1 UNIQUE (subtitleId, char),
    CHECK ((aozoraId IS NOT NULL) <> (subtitleId IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS training_data.CharacterSequences (
    id serial,
    --
    first numeric NOT NULL,
    middle numeric NOT NULL,
    third numeric NOT NULL,
    --
    count decimal NOT NULL,
    -- extendable
    aozoraId int REFERENCES training_data.AozoraBooks (id),
    subtitleId int REFERENCES training_data.SubtitleData (id),
    CONSTRAINT depup_prevention_aozora_2 UNIQUE (aozoraId, FIRST, middle, third),
    CONSTRAINT depup_prevention_subtitle_2 UNIQUE (subtitleId, FIRST, middle, third),
    CHECK ((aozoraId IS NOT NULL) <> (subtitleId IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS training_data.KanjiSequences (
    id serial PRIMARY KEY,
    characterSequence numeric[],
    count decimal NOT NULL,
    -- extendable
    aozoraId int REFERENCES training_data.AozoraBooks (id),
    subtitleId int REFERENCES training_data.SubtitleData (id),
    CONSTRAINT depup_prevention_aozora_3 UNIQUE (aozoraId, characterSequence),
    CONSTRAINT depup_prevention_subtitle_3 UNIQUE (subtitleId, characterSequence),
    CHECK ((aozoraId IS NOT NULL) <> (subtitleId IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS ccidx ON training_data.CharacterCounts (char, count);

-- Update as we go along.
CREATE VIEW training_data.AllTextSources AS
SELECT
    id,
    data,
    'Subtitle' AS sourceName,
    0 AS sourceId
FROM
    training_data.subtitledata
UNION ALL
SELECT
    id,
    data,
    'Aozora' AS sourceName,
    1 AS sourceId
FROM
    training_data.AozoraBooks;

CREATE OR REPLACE PROCEDURE Insert_Into_CharacterCount (p_char numeric, p_count numeric, p_isKanji boolean, p_table numeric, p_id numeric)
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_table = 0 THEN
        INSERT INTO training_data.CharacterCounts (char, count, isKanji, subtitleId)
            VALUES (p_char, p_count, p_isKanji, p_id);
    ELSIF p_table = 1 THEN
        INSERT INTO training_data.CharacterCounts (char, count, isKanji, aozoraId)
            VALUES (p_char, p_count, p_isKanji, p_id);
    ELSE
        RAISE EXCEPTION 'Unknown table type %', p_table;
    END IF;
END;
$$;

CREATE OR REPLACE PROCEDURE Insert_Into_KanjiSequences (p_chars numeric[], p_count numeric, p_table numeric, p_id numeric)
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_table = 0 THEN
        INSERT INTO training_data.KanjiSequences (characterSequence, count, subtitleId)
            VALUES (p_chars, p_count, p_id);
    ELSIF p_table = 1 THEN
        INSERT INTO training_data.KanjiSequences (char, count, aozoraId)
            VALUES (p_chars, p_count, p_id);
    ELSE
        RAISE EXCEPTION 'Unknown table type %', p_table;
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION Array_To_Int (arr numeric[])
    RETURNS text
    LANGUAGE plpgsql
    AS $$
BEGIN
    RETURN array_to_string(ARRAY (
            SELECT
                chr(num::int)
            FROM unnest(arr) AS num), '');
END;
$$;

CREATE MATERIALIZED VIEW training_data.CharacterCountsSums AS
SELECT
    char,
    SUM(count) AS sum_count
FROM
    training_data.CharacterCounts
GROUP BY
    char;

SELECT
    *
FROM
    training_data.alltextsources AS ats
    LEFT JOIN training_data.charactersequences AS cs ON ((cs.aozoraId = ats.id
                AND sourceName = 'Aozora')
            OR (cs.subtitleId = ats.id
                AND sourceName = 'Subtitle'))
WHERE
    cs.id IS NULL
    AND length(data) > 0;

