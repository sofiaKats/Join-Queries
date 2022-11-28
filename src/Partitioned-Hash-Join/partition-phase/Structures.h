#pragma once
#include <iostream>
#include "../Relation/Relation.hpp"
#include "../hopscotch/src/Hashtable.h"

using namespace std;

typedef struct Tuple {
  int32_t key = 0;
  int32_t payload = 0;
} Tuple;

typedef struct RelColumn {
  Tuple* tuples;
  uint32_t num_tuples;

  RelColumn(uint32_t size){
    tuples = new Tuple[size];
    num_tuples = size;
    for (int i = 0; i < size; i++)
      tuples[i].key = i;
  }
  ~RelColumn(){
    delete[] tuples;
  }
} RelColumn;

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
  RelColumn* rel = NULL;
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

typedef struct MatchRow{
  uint32_t* arr;
  uint32_t size;
  MatchRow(uint32_t size){
    this->size = size;
    arr = new uint32_t[size];
  }
} MatchRow;

typedef struct UsedRelations{
  MatchRow** matchRow;
  uint32_t size;
  uint32_t rowSize;
  UsedRelations(uint32_t size, uint32_t rowSize){
    this->size = size;
    this->rowSize = rowSize;
    matchRow = new MatchRow*[size];
    for (int i = 0; i < size; i++){
      matchRow[i] = new MatchRow(rowSize);
    }
  }
  ~UsedRelations(){
    for (int i = 0; i < size; i++){
      delete matchRow[i];
    }
    delete[] matchRow;
  }
} UsedRelations;
