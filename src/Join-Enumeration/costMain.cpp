#include <iostream>
#include "inttypes.h"
#include "../Joiner/Joiner.hpp"
#include "Cost.hpp"
#include <time.h>

int main(int argc, char* argv[]){
  clock_t start, end;
  Joiner *joiner;
  Parser parser;
  Queries *queries;
  Rels* relations;
  Cost* cost;

  cout << "==== Loading files...\n\n";
  relations = parser.OpenRelFileAndParse();
  cout << "  -- loaded rel files\n";
  joiner = new Joiner(relations->size);
  for (int i = 0; i<relations->size; i++){
    joiner->AddRelation(relations->paths[i]);
  }
  
  queries = parser.OpenQueryFileAndParse();
  cout << "  -- loaded query files\n";

  cost = new Cost(queries->queries_arr[0], joiner->relations);

  exit(1); // we need munmap to be called to free mapped memory
}