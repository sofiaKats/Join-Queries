#include <iostream>
#include "inttypes.h"
#include "./Joiner/Joiner.hpp"

int main(int argc, char* argv[]) {
  srand(time(NULL));

  Joiner* joiner;
  Parser parser;
  Query* query;
  char input[20], line[50];
  memset(input, '\0', 20); memset(line, '\0', 50);
  int filesCount;

  filesCount = 13;
  joiner = new Joiner(filesCount);

  try{
    for (int i=0; i<=filesCount; i++){
      sprintf(line, "./workloads/small/r%d", i);
      joiner->AddRelation(line);
      cout << "opened file: " << line << endl;
    }

  /*cout << "--- Insert num of files ---\n";
  scanf("%" SCNd32, &filesCount);
  joiner = new Joiner(filesCount, 20000);

  cout << ">>> Insert Relations:" << endl;
  try{
    while (filesCount){
      scanf("%s", input);
      if (!strcmp(input, "Done")) break;
      try{
        sprintf(line, "./workloads/small/%s", input);
        joiner->AddRelation(line);
        filesCount--;
      }
      catch(const exception& e){
        cout << e.what() << endl;
      }
    }*/

    // Preparation phase (not timed)
    // Build histograms, indexes,...

    cout << ">>> Insert Queries:" << endl;

    while(1){
      scanf("%s", input);
      if (!strcmp(input, "Done")) break;
      try{
        sprintf(line, "./workloads/small/%s", input);
        query = parser.OpenFileAndParse();
        string results = joiner->Join(*query);
        cout << "\n--- Join Results ---\n\n" << results << endl;
      }
      catch(const exception& e){
        cout << e.what() << endl;
      }
    }

    delete joiner;
    return 0;
  }
  catch (const exception& e){
    cout << e.what() << endl;
    delete joiner;
    return 1;
  }
}
