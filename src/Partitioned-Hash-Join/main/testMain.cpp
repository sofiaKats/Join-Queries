#include "../partition-phase/PartitionedHashJoin.h"
#include "Parser.h"
#include <dirent.h>

using namespace std;

int main(int argc, char** argv){

  Parser* parser = new Parser();
  Relation *relR, *relS;

  if (argc < 2){
    cout << "[Main] Usage: ./program <Relation Folder> \n";
    return 0;
  }

  int relIdsLength = 0, relArrayLength = 0;
  Relation** relArray = parser->ReadRelations(argv[1], relArrayLength);
  int** relIdsToJoin = parser->ReadQueries(relIdsLength);

  for (int i = 0; i < relIdsLength; i++){
    cout << "\n\n>>>>>>> Running Query line no: " << i << " with relations - " << relIdsToJoin[i][0] << " : "<< relIdsToJoin[i][1] << endl;
    relR = relArray[relIdsToJoin[i][0]];
    relS = relArray[relIdsToJoin[i][1]];

    PartitionedHashJoin* phj = new PartitionedHashJoin(relR, relS);
    phj->Solve();

    delete phj;
  }

  for(int i = 0; i < relIdsLength; i++){
    delete[] relIdsToJoin[i];
  }
  delete[] relIdsToJoin;

  for(int i = 0; i < relArrayLength; i++){
    delete relArray[i];
  }
  delete[] relArray;

  delete parser;
}
