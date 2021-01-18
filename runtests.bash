#!/bin/bash
source env.sh
source local_s3_run.bash

pytest tests

source local_s3_delete.bash