#!/bin/bash
source env.bash
source local_s3_run.bash

cd ..
pytest tests
cd -
source local_s3_delete.bash