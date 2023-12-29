#!/bin/bash

python3 ./data/read_data.py &&
python3 ./data/clean_data.py &&
python3 ./data/json_schemas.py

