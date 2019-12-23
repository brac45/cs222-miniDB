#!/usr/bin/env bash

echo "Building RM .."

make clean
make

find . -type f -name '*.tbl' -exec rm -vf {} +
find . -type f -name '*.txt' -exec rm -vf {} +
find . -type f -name '*.time' -exec rm -vf {} +

echo "Running RM .."

echo "Running rmtest_create_tables .."
{ time ./rmtest_create_tables > rmtest_create_tables.txt 2>&1 ; } 2> create.time
exit_status=$?
if [ $exit_status -ne 0 ]; then
  echo "========================+=========================="
  echo "    (FAILED) failed with ${exit_status}!"
  echo "========================+=========================="
fi

{ time ./rmtest_00 2> rmtest_00.txt 2>&1 ; } 2> rmtest_00.time
{ time ./rmtest_01 2> rmtest_01.txt 2>&1 ; } 2> rmtest_01.time
{ time ./rmtest_02 2> rmtest_02.txt 2>&1 ; } 2> rmtest_02.time
{ time ./rmtest_03 2> rmtest_03.txt 2>&1 ; } 2> rmtest_03.time
{ time ./rmtest_04 2> rmtest_04.txt 2>&1 ; } 2> rmtest_04.time
{ time ./rmtest_05 2> rmtest_05.txt 2>&1 ; } 2> rmtest_05.time
{ time ./rmtest_06 2> rmtest_06.txt 2>&1 ; } 2> rmtest_06.time
{ time ./rmtest_07 2> rmtest_07.txt 2>&1 ; } 2> rmtest_07.time
{ time ./rmtest_08 2> rmtest_08.txt 2>&1 ; } 2> rmtest_08.time
{ time ./rmtest_09 2> rmtest_09.txt 2>&1 ; } 2> rmtest_09.time
{ time ./rmtest_10 2> rmtest_10.txt 2>&1 ; } 2> rmtest_10.time
{ time ./rmtest_11 2> rmtest_11.txt 2>&1 ; } 2> rmtest_11.time
{ time ./rmtest_12 2> rmtest_12.txt 2>&1 ; } 2> rmtest_12.time
{ time ./rmtest_13 2> rmtest_13.txt 2>&1 ; } 2> rmtest_13.time
{ time ./rmtest_14 2> rmtest_14.txt 2>&1 ; } 2> rmtest_14.time

echo "Running rmtest_delete_tables .."
{ time ./rmtest_delete_tables > rmtest_d_tables.txt 2>&1 ; } 2> delete.time
exit_status=$?
if [ $exit_status -ne 0 ]; then
  echo "========================+=========================="
  echo "    (FAILED) failed with ${exit_status}!"
  echo "========================+=========================="
fi

make clean