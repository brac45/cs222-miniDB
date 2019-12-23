#!/usr/bin/env bash

echo "Building QE .."

make clean
make

find . -maxdepth 1 -type f -name '*.tbl' -exec rm -vf {} +
find . -maxdepth 1 -type f -name '*.txt' -exec rm -vf {} +
find . -maxdepth 1 -type f -name '*.time' -exec rm -vf {} +

echo "Running QE .."

{ time ./qetest_01 2>&1 | tee -a qetest_01.txt ; } 2> qetest_01.time
{ time ./qetest_02 2>&1 | tee -a qetest_02.txt ; } 2> qetest_02.time
{ time ./qetest_03 2>&1 | tee -a qetest_03.txt ; } 2> qetest_03.time
{ time ./qetest_04 2>&1 | tee -a qetest_04.txt ; } 2> qetest_04.time
{ time ./qetest_05 2>&1 | tee -a qetest_05.txt ; } 2> qetest_05.time
{ time ./qetest_06 2>&1 | tee -a qetest_06.txt ; } 2> qetest_06.time
{ time ./qetest_07 2>&1 | tee -a qetest_07.txt ; } 2> qetest_07.time
{ time ./qetest_08 2>&1 | tee -a qetest_08.txt ; } 2> qetest_08.time
{ time ./qetest_09 2>&1 | tee -a qetest_09.txt ; } 2> qetest_09.time
{ time ./qetest_10 2>&1 | tee -a qetest_10.txt ; } 2> qetest_10.time
{ time ./qetest_11 2>&1 | tee -a qetest_11.txt ; } 2> qetest_11.time
{ time ./qetest_12 2>&1 | tee -a qetest_12.txt ; } 2> qetest_12.time
{ time ./qetest_13 2>&1 | tee -a qetest_13.txt ; } 2> qetest_13.time
{ time ./qetest_14 2>&1 | tee -a qetest_14.txt ; } 2> qetest_14.time
{ time ./qetest_15 2>&1 | tee -a qetest_15.txt ; } 2> qetest_15.time
{ time ./qetest_16 2>&1 | tee -a qetest_16.txt ; } 2> qetest_16.time

cat qetest_01.time
cat qetest_02.time
cat qetest_03.time
cat qetest_04.time
cat qetest_05.time
cat qetest_06.time
cat qetest_07.time
cat qetest_08.time
cat qetest_09.time
cat qetest_10.time
cat qetest_11.time
cat qetest_12.time
cat qetest_13.time
cat qetest_14.time
cat qetest_15.time
cat qetest_16.time

{ time ./qetest_p00 2>&1 | tee -a qetest_p00.txt ; } 2> qetest_p00.time
{ time ./qetest_p01 2>&1 | tee -a qetest_p01.txt ; } 2> qetest_p01.time
{ time ./qetest_p02 2>&1 | tee -a qetest_p02.txt ; } 2> qetest_p02.time
{ time ./qetest_p03 2>&1 | tee -a qetest_p03.txt ; } 2> qetest_p03.time
{ time ./qetest_p04 2>&1 | tee -a qetest_p04.txt ; } 2> qetest_p04.time
{ time ./qetest_p05 2>&1 | tee -a qetest_p05.txt ; } 2> qetest_p05.time
{ time ./qetest_p06 2>&1 | tee -a qetest_p06.txt ; } 2> qetest_p06.time
#{ time ./qetest_p07 2>&1 | tee -a qetest_p07.txt ; } 2> qetest_p07.time
#{ time ./qetest_p08 2>&1 | tee -a qetest_p08.txt ; } 2> qetest_p08.time
{ time ./qetest_p09 2>&1 | tee -a qetest_p09.txt ; } 2> qetest_p09.time
{ time ./qetest_p10 2>&1 | tee -a qetest_p10.txt ; } 2> qetest_p10.time
{ time ./qetest_p11 2>&1 | tee -a qetest_p11.txt ; } 2> qetest_p11.time
{ time ./qetest_p12 2>&1 | tee -a qetest_p12.txt ; } 2> qetest_p12.time

cat qetest_p00.time
cat qetest_p01.time
cat qetest_p02.time
cat qetest_p03.time
cat qetest_p04.time
cat qetest_p05.time
cat qetest_p06.time
#cat qetest_p07.time
#cat qetest_p08.time
cat qetest_p09.time
cat qetest_p10.time
cat qetest_p11.time
cat qetest_p12.time
