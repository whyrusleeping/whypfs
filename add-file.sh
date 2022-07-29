#!/bin/bash

fname=$(basename $1)

curl --progress-bar -X POST -F "data=@$1" -F "name=$fname" http://localhost:5005/unixfs/add
