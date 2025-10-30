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
    character text CHECK (char_length(character) = 1) NOT NULL,
    count decimal NOT NULL,
    isKanji boolean NOT NULL,
    -- extendable
    aozoraId int REFERENCES training_data.AozoraBooks (id) UNIQUE,
    subtitleId int REFERENCES training_data.SubtitleData (id) UNIQUE,
    CHECK (aozoraId IS NOT NULL AND subtitleId IS NOT NULL)
);

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
    IF p_table = 1 THEN
        INSERT INTO training_data.CharacterCounts (char, count, isKanji, subtitleId)
            VALUES (p_char, p_count, p_isKanji, p_id);
    ELSIF p_table = 2 THEN
        INSERT INTO training_data.CharacterCounts (char, count, isKanji, aozoraId)
            VALUES (p_char, p_count, p_isKanji, p_id);
    ELSE
        RAISE EXCEPTION 'Unknown table type %', p_table;
    END IF;
END;
$$;

