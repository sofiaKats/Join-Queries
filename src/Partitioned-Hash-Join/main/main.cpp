#include <iostream>
#include "inttypes.h"
#include "./Joiner/Joiner.hpp"

int main(int argc, char* argv[]) {
  srand(time(NULL));

  Joiner *joiner;
  Parser parser;
  Query *query;
  char input[20]; memset(input, '\0', 20);
  FILE *fp;
  char *line = NULL;
  char path[50];
  size_t len = 0;
  ssize_t read;
  unsigned filesCount;

  fp = fopen("./workloads/small/small.init", "r");
  if (fp == NULL){
      cout << "Could not open init file" << endl;
      exit(EXIT_FAILURE);
  }
  while ((read = getline(&line, &len, fp)) != -1) { //count lines to allocate relation table
    if (read == 0) break;
    ++filesCount;
  }
  joiner = new Joiner(filesCount);
  rewind(fp);
  try{
    while ((read = getline(&line, &len, fp)) != -1) { //count lines to allocate relation table
      if (read == 0) break;
      line[strlen(line)-1] = '\0';
      sprintf(path, "./workloads/small/%s", line);
      joiner->AddRelation(path);
      cout << "opened file: " << path << endl;
    }

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
