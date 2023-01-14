#include <iostream>
#include "inttypes.h"
#include "../Joiner/Joiner.hpp"
//#include "../Join-Enumeration/JoinEnum.hpp"
#include <time.h>

int main(int argc, char* argv[]){
  struct timespec start, end;
  Joiner *joiner;
  Parser parser;
  Queries *queries;
  Rels* relations;
  char** out;

  cout << "==== Loading files...\n\n";
  relations = parser.OpenRelFileAndParse();
  cout << "  -- loaded rel files\n";
  joiner = new Joiner(relations->size);
  for (int i = 0; i<relations->size; i++){
    joiner->AddRelation(relations->paths[i]);
  }

  queries = parser.OpenQueryFileAndParse();
  cout << "  -- loaded query files\n";

  cout << "\n==== Running queries...\n\n";
  out = new char*[queries->size];
  clock_gettime(CLOCK_MONOTONIC, &start);
  for (int i = 0; i < queries->size; i++){
    if (queries->queries_arr[i] == nullptr) continue;
    JoinEnum* jn = new JoinEnum(queries->queries_arr[i], joiner->relations, relations->size);
    jn->reassignPriority(queries->queries_arr[i], jn->DP_linear());
    jn->reassignPrdctOrder();
    delete jn;
  }

  for (int i = 0; i < queries->size; i++)
    sch.submit_job(new Job(joiner->thread_executeQuery, (void*)new JoinerArgs(joiner, queries->queries_arr[i], out, i, joiner->relations, joiner->numRelations)));

  sch.wait_all_tasks_finish();

  clock_gettime(CLOCK_MONOTONIC, &end);
  double duration = (end.tv_sec - start.tv_sec);
  duration += (end.tv_nsec - start.tv_nsec) / 1000000000.0;

  for (int i = 0; i<queries->size; i++){
    cout << out[i] << endl;
    delete out[i];
  }
  delete[] out;

  cout << "\n==== Run in ~" << duration << " sec\n";
  sch.destroy_scheduler();
  delete joiner;
  return 0;
}
