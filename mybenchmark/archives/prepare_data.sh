#!/bin/bash

hpcc upload_data --data benchmark/dataset/Anagram2/2of12.txt
hpcc spray 2of12.txt thor::word_list_csv --dstcluster mythor --format delimited --maxrecordsize 8192 --separator \\ --terminator \\n,\\r\\n --quote \'
roxie publish validateanagrams  --ecl benchmark/Anagram2/anagram2.ecl


hpcc upload_data --data benchmark/dataset/OriginalPerson/OriginalPerson
hpcc spray OriginalPerson tutorial::YN::OriginalPerson --dstcluster mythor --recordsize 124
thor run --ecl benchmark/OriginalPerson/BWR_ProcessRawData.ecl --wait_until_complete
thor run --ecl benchmark/OriginalPerson/TutorialYourName/BWR_BuildPeopleByZip.ecl --dir benchmark/OriginalPerson --wait_until_complete
roxie publish fetchpeoplebyzipservice --ecl benchmark/OriginalPerson/TutorialYourName/FetchPeopleByZipService.ecl --dir benchmark/OriginalPerson


hpcc upload_data --data benchmark/dataset/SixDegree/actors.list
hpcc upload_data --data benchmark/dataset/SixDegree/actresses.list
hpcc spray actors.list ~thor::in::IMDB::actors.list --dstcluster=mythor --format=delimited --maxrecordsize=8192 --separator=\\, --terminator=\\n,\\r\\n --quote=\' --overwrite --replicate
hpcc spray actresses.list ~thor::in::IMDB::actresses.list --dstcluster=mythor --format=delimited --maxrecordsize=8192 --separator=\\, --terminator=\\n,\\r\\n --quote=\' --overwrite --replicate
thor run --ecl benchmark/SixDegree/BuildIndex.ecl --wait_until_complete
roxie publish searchlinks --ecl benchmark/SixDegree/SearchLinks.ecl
