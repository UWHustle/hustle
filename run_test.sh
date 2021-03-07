#!/usr/bin/env bash

cd test/data && python3 test_generate_data.py
cd ../../build && ctest --output-on-failure
