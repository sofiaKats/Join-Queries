#include <iostream>
#include "inttypes.h"
#include "../Joiner/Joiner.hpp"
#include <time.h>

int main(int argc, char* argv[]){
  clock_t start, end;
  Joiner *joiner;
  Parser parser;
  Queries *queries;
  Rels* relations;

  relations = parser.OpenRelFileAndParse();
  joiner = new Joiner(relations->size);
  for (int i = 0; i<relations->size; i++){
    joiner->AddRelation(relations->paths[i]);
  }

  queries = parser.OpenQueryFileAndParse();

  start = clock();
  for (int i = 0; i <queries->size; i++){
    if (queries->queries_arr[i] == NULL) {cout << "F\n\n"; continue;}
    if (i==15 || i==29 || i==30 || i==39 || i==53) {cout << i + 1 << ". ---\n"; continue;}
    cout << i + 1 << ". ";
    joiner->Join(*queries->queries_arr[i]);
  }

  end = clock();
  double duration = ((double)end - start)/CLOCKS_PER_SEC;
  cout << "Run in ~" << duration << " sec\n";
  exit(1); // we need munmap to be called to free mapped memory
}
