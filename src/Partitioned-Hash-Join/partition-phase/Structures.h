#pragma once

#include <iostream>
#include <cstdint>
#include "../Relation/Relation.hpp"
#include "../hopscotch/src/Hashtable.h"

using namespace std;

typedef struct Tuple {
  int32_t key = 0;
  int32_t payload = 0;
} Tuple;

/**
* Type definition for a relation.
* It consists of an array of tuples and a size of the relation.
*/
typedef struct Column {
  Tuple* tuples;
  uint32_t num_tuples;

  Column(uint32_t size){
    tuples = new Tuple[size];
    num_tuples = size;
  }
  ~Column(){
    delete[] tuples;
  }
} Column;

typedef struct Hist{
  uint32_t length;
  uint32_t usedLength = 0;
  int* arr;

  Hist(uint32_t size){
    arr = new int[size]{};
    length = size;
  }
  ~Hist(){
    delete[] arr;
  }
} Hist;

typedef struct PrefixSum{
  uint32_t length;
  int** arr;

  PrefixSum(uint32_t size){
    arr = new int*[size];
    length = size;

    for (uint32_t i = 0; i < length; i++){
      arr[i] = new int[2]{};
    }
  }
  ~PrefixSum(){
    for (uint32_t i = 0; i < length; i++){
      delete[] arr[i];
    }
    delete[] arr;
  }
} PrefixSum;

typedef struct Part{
  Column* rel = NULL;
  PrefixSum* prefixSum = NULL;
  Hashtable** hashtables = NULL;
  ~Part(){
    if (hashtables != NULL){
      for (int i = 0; i < prefixSum->length - 1; i++){
        if(prefixSum->arr[i][0]==-1) break;
        delete hashtables[i];
      }
      delete[] hashtables;
    }
    else
      delete hashtables;
    delete prefixSum;
    delete rel;
  }
} Part;
