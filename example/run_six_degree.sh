#!/bin/bash

# 1) Upload data
hpcc upload_data --data benchmark/dataset/SixDegree/actors.list
hpcc upload_data --data benchmark/dataset/SixDegree/actresses.list

# 2) Spray the data
hpcc spray actors.list ~thor::in::IMDB::actors.list --dstcluster=mythor --format=delimited --maxrecordsize=8192 --separator=\\, --terminator=\\n,\\r\\n --quote=\' --overwrite --replicate
hpcc spray actresses.list ~thor::in::IMDB::actresses.list --dstcluster=mythor --format=delimited --maxrecordsize=8192 --separator=\\, --terminator=\\n,\\r\\n --quote=\' --overwrite --replicate

# 3) Test to output data (will fail due to too large size)
thor run --ecl benchmark/SixDegree/OutputFileActors.ecl

# 4) Test to count the numbers
thor run --ecl benchmark/SixDegree/CountNumberSets.ecl --wait_until_complete

# 5) Build the index required for Roxie
thor run --ecl benchmark/SixDegree/BuildIndex.ecl --wait_until_complete

# 6) Run a on-the-fly query without publishing it.
roxie run --ecl benchmark/SixDegree/SearchLinks.ecl --input name 'Everingham, Andi'

# 7) Publish the query
roxie publish searchlinks --ecl benchmark/SixDegree/SearchLinks.ecl

# 8) Run a published query against a user-defined name.
roxie query searchlinks --input name 'Everingham, Andi'
