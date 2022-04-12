#!/bin/bash

APP_PATH=/eyeCloudAI/app/ape

export PYTHONPATH=$PYTHONPATH:$APP_PATH/da/lib:$APP_PATH/da
export PYTHONPATH=$PYTHONPATH:$APP_PATH/pycmmn/lib:$APP_PATH/pycmmn

DATASET_ID=${1}
WORKER_IDX=${3}

if [ "${2}" = "chief" ]
then
  /usr/local/bin/python3.7 -m dataanalyzer.DataAnalyzerChief ${DATASET_ID} ${WORKER_IDX}
else
  /usr/local/bin/python3.7 -m dataanalyzer.DataAnalyzerWorker ${DATASET_ID} ${WORKER_IDX}
fi
