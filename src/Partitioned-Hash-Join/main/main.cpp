#include <iostream>
#include "inttypes.h"
#include "./Joiner/Joiner.hpp"

// int main(int argc, char* argv[]) {
//   srand(time(NULL));

//   Joiner *joiner;
//   Parser parser;
//   Queries *queries;
//   char input[20]; memset(input, '\0', 20);
//   FILE *fp;
//   char *line = NULL;
//   char path[50];
//   size_t len = 0;
//   ssize_t read;
//   unsigned filesCount = 0;

//   fp = fopen("./workloads/small/small.init", "r");
//   if (fp == NULL){
//       cout << "Could not open init file" << endl;
//       exit(EXIT_FAILURE);
//   }
//   while ((read = getline(&line, &len, fp)) != -1) { //count lines to allocate relation table
//     if (read == 0) break;
//     ++filesCount;
//   }
//   joiner = new Joiner(filesCount);
//   rewind(fp);
//   try{
//     while ((read = getline(&line, &len, fp)) != -1) { //count lines to allocate relation table
//       if (read == 0) break;
//       line[strlen(line)-1] = '\0';
//       sprintf(path, "./workloads/small/%s", line);
//       joiner->AddRelation(path);
//       cout << "opened file: " << path << endl;
//     }

//     cout << ">>> Insert Queries:" << endl;

//     while(1){
//       scanf("%s", input);
//       if (!strcmp(input, "Done")) break;
//       try{
//         //sprintf(line, "./workloads/small/%s", input);
//         queries = parser.OpenQueryFileAndParse();
//         //cout << "MAINNNNNNNNNNNNNNNNN" <<  endl;
//         for (int i = 0; i < queries->size; i++){
//           if (queries->queries_arr[i] == NULL) {cout << "End of batch!"; break;}
//           joiner->Join(*queries->queries_arr[i]);
//         }
//         //joiner->Join(*queries->queries_arr[2]);
//         break;
//         //string results = joiner->Join(*query);
//         //cout << "\n--- Join Results ---\n\n" << results << endl;
//       }
//       catch(const exception& e){
//         cout << e.what() << endl;
//       }
//     }

//     delete joiner;
//     return 0;
//   }
//   catch (const exception& e){
//     cout << e.what() << endl;
//     delete joiner;
//     return 1;
//   }
// }

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
    if (queries->queries_arr[i] == NULL) {cout << "End of batch!\n"; break;}
    joiner->Join(*queries->queries_arr[i]);
  }  
}
