create schema if not exists training_data;

create Table if not exists training_data.SubtitleData(
  id SERIAL PRIMARY KEY,
  data text not null,
  path text not null unique
);

create Table if not exists training_data.AozoraBooks(
  id SERIAL PRIMARY KEY,
  aozoraName text not null,
  aozoraId decimal not null unique,
  difficulty decimal not null ,
  data text not null,
  year int not null
);

