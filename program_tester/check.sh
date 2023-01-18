#!/bin/bash

result="../../program_tester/result.txt"
file1="../../program_tester/correct_result.txt"
rm -f $result
cat > $result
if diff -u "$file1" "$result"; then
  echo "-- ok --"
else
  echo "-- wrong results--"
fi
