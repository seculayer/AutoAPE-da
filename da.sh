#!/bin/bash

export PYTHONPATH=PYTHONPATH:/eyeCloudAI/app/ape/da/lib

DATASET_ID=${1}
WORKER_IDX=${3}

if [ "${2}" = "chief" ]
then
  /usr/local/bin/python3.7 -m dataanalyzer.DataAnalyzerChief ${DATASET_ID} ${WORKER_IDX}
else
  /usr/local/bin/python3.7 -m dataanalyzer.DataAnalyzerWorker ${DATASET_ID} ${WORKER_IDX}
fi
