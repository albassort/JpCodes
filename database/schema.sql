create schema if not exists training_data;
Create Table if not exists training_data.SubtitleData(
  id SERIAL PRIMARY KEY,
  data text not null,
  path text not null unique
);



