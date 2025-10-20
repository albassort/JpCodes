SELECT DISTINCT
    (texts.作品ID),
    常用平均値 AS dif,
    CAST(substr(底本初版発行年1, 0, 5) AS int) AS year,
    works.作品名,
    texts.本文
FROM
    texts
    JOIN works ON works.作品ID = texts.作品ID
    JOIN difficulty ON difficulty.作品ID = texts.作品ID
WHERE
    dif > 1
    AND year > 1500
