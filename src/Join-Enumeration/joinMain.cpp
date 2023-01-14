#include <iostream>
#include "inttypes.h"
#include "JoinEnum.hpp"
#include "../Structures/Relation/Relation.hpp"
//#include "Cost.hpp"
#include <time.h>

void AddRelation(const char* fileName, Relation** relations);

int main(int argc, char* argv[]){
  clock_t start, end;
  Parser parser;
  Queries *queries;
  Rels* relations;
  Relation** r;
  Cost* cost;
  JoinEnum* jn;

  cout << "==== Loading files...\n\n";
  relations = parser.OpenRelFileAndParse();
  cout << "  -- loaded rel files\n"; 

  
  queries = parser.OpenQueryFileAndParse();
  cout << "  -- loaded query files\n";

  r = new Relation*[relations->size]{};
  for (int i = 0; i < relations->size; i++){
    r[i] = new Relation(relations->paths[i]);
  }

  for (int i = 0; i < queries->size; i++){
    if (queries->queries_arr[i] == nullptr) continue;
    JoinEnum* jn = new JoinEnum(queries->queries_arr[i], r, relations->size);
    jn->reassignPriority(queries->queries_arr[i], jn->DP_linear());
    jn->reassignPrdctOrder();
    delete jn;
  }

  exit(1); // we need munmap to be called to free mapped memory
}
void AddRelation(const char* fileName, Relation** relations){
  static int numRelations = 0;
  relations[numRelations++] = new Relation(fileName);
}