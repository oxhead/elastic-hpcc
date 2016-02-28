#!/bin/bash

# 1) upload data
hpcc upload_data --data benchmark/dataset/OriginalPerson/OriginalPerson

# 2) spray data
hpcc spray OriginalPerson tutorial::YN::OriginalPerson --dstcluster mythor --recordsize 124

# 3) process data
thor run --ecl benchmark/OriginalPerson/BWR_ProcessRawData.ecl

# 4) build index
thor run --ecl benchmark/OriginalPerson/TutorialYourName/BWR_BuildPeopleByZip.ecl --dir benchmark/OriginalPerson

# 5) publish query
roxie publish fetchpeoplebyzipservice --ecl benchmark/OriginalPerson/TutorialYourName/FetchPeopleByZipService.ecl --dir benchmark/OriginalPerson

# 6) submit a Roxie query
roxie query fetchpeoplebyzipservice --query ZIPValue 27617
