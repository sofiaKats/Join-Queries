#include <iostream>
#include "./Joiner/Joiner.hpp"
#include "./Parsing/Parser.hpp"

// TO DO
// REMOVE STRING STL

int main(int argc, char* argv[]) {

  Joiner* joiner;
  char input[20], line[50];
  memset(input, '\0', 20); memset(line, '\0', 50);
  int filesCount;

  cout << "--- Insert num of files ---\n";
  scanf("%d", &filesCount);
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
    }

    // Preparation phase (not timed)
    // Build histograms, indexes,...

    cout << ">>> Insert Queries:" << endl;

    QueryInfo i;
    //string line2 = "3 0 1|0.2=1.0&0.1=2.0&0.2>3000|1.2 0.1";
    string line2 = "0 1 |0.2=1.0&0.2>3000|1.2 0.1";
    i.parseQuery(line2);

    string results = joiner->Join(i);

    if (results != "")
    cout << "\n--- Join Results ---\n\n" << results << endl;

    // while (getline(cin, line)) {
    //    cout << line<<endl;
    //    if (line == "F") continue; // End of a batch
    //    i.parseQuery(line);
    //    //cout << joiner.join(i);
    // }
    //cout << i.predicates[0].left.relId<<endl;
    //cout << i.predicates[0].right.relId<<endl;

    delete joiner;

    return 0;
  }
  catch (const exception& e){
    cout << e.what() << endl;
    delete joiner;
    return 1;
  }
}
