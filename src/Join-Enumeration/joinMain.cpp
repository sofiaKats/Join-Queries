#include <iostream>
#include "inttypes.h"
#include "JoinEnum.hpp"
#include <time.h>

int main(int argc, char* argv[]){
  clock_t start, end;
  Parser parser;
  Queries *queries;
  Rels* relations;
  Cost* cost;
  JoinEnum* jn;

  cout << "==== Loading files...\n\n";
  relations = parser.OpenRelFileAndParse();
  cout << "  -- loaded rel files\n";
  
  queries = parser.OpenQueryFileAndParse();
  cout << "  -- loaded query files\n";

  jn = new JoinEnum(queries->queries_arr[0]);
  jn->DP_linear();

  exit(1); // we need munmap to be called to free mapped memory
}