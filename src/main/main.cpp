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
  bool testMode = false;

  if (argc > 1 && !strcmp(argv[1], "-t"))
    testMode = true;
  else if (argc > 1){
    cout << "usage: ./program [-t|testing]\n";
    return -1;
  }
  //Load relations and queries
  if (!testMode) cout << "==== Loading files...\n\n";
  relations = parser.OpenRelFileAndParse();
  if (!testMode) cout << "  -- loaded rel files\n";
  joiner = new Joiner(relations->size);
  for (int i = 0; i<relations->size; i++){
    joiner->AddRelation(relations->paths[i]);
  }
  queries = parser.OpenQueryFileAndParse();
  if (!testMode) cout << "  -- loaded query files\n";
  //Load join enumeration
  for (int i = 0; i < queries->size; i++){
    if (queries->queries_arr[i] == nullptr) continue;
    JoinEnum* jn = new JoinEnum(queries->queries_arr[i], joiner->relations, relations->size);
    jn->reassignPriority(queries->queries_arr[i], jn->DP_linear());
    jn->reassignPrdctOrder();
    delete jn;
  }
  //Run queries
  if (!testMode) cout << "\n==== Running queries...\n\n";
  out = new char*[queries->size]{};

  clock_gettime(CLOCK_MONOTONIC, &start);

  for (int i = 0; i < queries->size; i++)
    sch.submit_job(new Job(joiner->thread_executeQuery, (void*)new JoinerArgs(joiner, queries->queries_arr[i], out, i, joiner->relations, joiner->numRelations)));
  sch.wait_all_tasks_finish();

  clock_gettime(CLOCK_MONOTONIC, &end);
  double duration = (end.tv_sec - start.tv_sec);
  duration += (end.tv_nsec - start.tv_nsec) / 1000000000.0;

  for (int i = 0; i<queries->size; i++){
    if(out[i] == NULL) continue;
    cout << out[i] << endl;
    delete[] out[i];
  }
  delete[] out;

  if (!testMode) cout << "\n==== Run in ~" << duration << " sec\n";
  sch.destroy_scheduler();
  delete joiner;
  return 0;
}
