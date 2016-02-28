#!/bin/bash

# 1) Publish the query
roxie publish validateanagrams  --ecl benchmark/roxie/anagram2.ecl

# 2) Run a published query with a user-defined word
roxie query validateanagrams --query word test
