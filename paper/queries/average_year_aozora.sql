WITH years AS (
  select texts.作品ID,  常用平均値 as dif,
  CAST(substr(底本初版発行年1, 0, 5) AS real) AS year,
  works.作品名, texts.本文,
  LENGTH(本文) as text_size

  from texts 
  join works on works.作品ID = texts.作品ID
  join difficulty on difficulty.作品ID = texts.作品ID

  where  dif > 1 and year > 1500
)
-- Weighted sum per text size.
SELECT sum(year*text_size) * 1.0 / sum(text_size), 
sum(text_size), 
avg(year)
FROM years
WHERE year > 1500 and  text_size > 0;

