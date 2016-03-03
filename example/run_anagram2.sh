#!/bin/bash

# 1) Upload data
hpcc upload_data --data benchmark/dataset/Anagram2/2of12.txt

# 2) Spary the data
hpcc spray 2of12.txt thor::word_list_csv --dstcluster mythor --format delimited --maxrecordsize 8192 --separator \\ --terminator \\n,\\r\\n --quote \'

# 1) Publish the query
roxie publish validateanagrams  --ecl benchmark/Anagram2/anagram2.ecl

# 2) Run a published query with a user-defined word
roxie query validateanagrams --input word test
