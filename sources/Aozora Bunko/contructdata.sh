#!/bin/bash


if [ -f ./aozora.db ]; then
  echo "database exists, work is already done!"
  exit 0;
fi


if [ ! -f ./aozora_corpus.db ]; then

  if [ !  -f ./archive.zip ]; then 
    echo "no db or archive... Exiting. Download the database here https://www.kaggle.com/datasets/ryancahildebrandt/azbcorpus"
    exit 1

  else 
    echo 'extracting archive.zip'
    unzip archive.zip
  fi
fi

if [ ! -f duckdb ]; then
  echo 'we need a specific version of duckdb. v0.8.1.'

  if [ ! -f ./duckdb_cli-linux-amd64.zip ]; then
    echo "downloading duckdb"
    wget 'https://github.com/duckdb/duckdb/releases/download/v0.8.1/duckdb_cli-linux-amd64.zip'
  fi

  echo "unzipping duckdb"
  unzip duckdb_cli-linux-amd64.zip

fi

if [ ! -f ./aozora.db ]; then

  echo 'making sqlite db'
  echo '.dump' | ./duckdb aozora_corpus.db | sqlite3 aozora.db

fi

rm meta_info.csv
rm aozora_corpus.csv
rm aozora_corpus.db
rm main_text.csv 
rm duckdb_cli-linux-amd64.zip
rm duckdb
rm archive.zip

