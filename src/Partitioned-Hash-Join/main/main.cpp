#include <iostream>
#include "inttypes.h"
#include "./Joiner/Joiner.hpp"

int main(int argc, char* argv[]){
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

  for (int i = 0; i < queries->size; i++){
    if (queries->queries_arr[i] == NULL) {cout << "F\n\n"; continue;}
    if (i==15 || i==25 || i==29 || i==30 || i==36 || i==39 || i==53) {cout << i + 1 << ". ---\n"; continue;}
    cout << i + 1 << ". ";
    joiner->Join(*queries->queries_arr[i]);
  }  
  //joiner->Join(*queries->queries_arr[29]);
}
