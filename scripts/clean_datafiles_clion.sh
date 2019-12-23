#!/usr/bin/env bash

find ../cmake-build-debug -maxdepth 1 -type f -name '*.tbl' -exec rm -vf {} +
find ../cmake-build-debug -maxdepth 1 -type f -name '*.idx' -exec rm -vf {} +