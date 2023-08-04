#!/bin/bash

APP_PATH=/eyeCloudAI/app/ape

DATASET_ID=${1}
WORKER_IDX=${3}

if [ -x "$APP_PATH/da/.venv/bin/python3" ]
then
  PYTHON_BIN="$APP_PATH/da/.venv/bin/python3"
else
  PYTHON_BIN="$(command -v python3.7)"
  export PYTHONPATH=$PYTHONPATH:$APP_PATH/da/lib:$APP_PATH/da
  export PYTHONPATH=$PYTHONPATH:$APP_PATH/eda/lib:$APP_PATH/eda
  export PYTHONPATH=$PYTHONPATH:$APP_PATH/pycmmn/lib:$APP_PATH/pycmmn
  export NLTK_DATA=$APP_PATH/eda/eda/resources/nltk
fi

if [ "${2}" = "chief" ]
then
  $PYTHON_BIN -m dataanalyzer.DataAnalyzerChief ${DATASET_ID} ${WORKER_IDX}
else
  $PYTHON_BIN -m dataanalyzer.DataAnalyzerWorker ${DATASET_ID} ${WORKER_IDX}
fi
