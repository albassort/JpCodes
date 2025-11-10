#!/bin/zsh

mkdir output/
for file in ../sources/subtitles/**/*.srt; do
  base=$(basename $file)
  cat "$file" | ./a.out > "output/$base" &

done

wait

