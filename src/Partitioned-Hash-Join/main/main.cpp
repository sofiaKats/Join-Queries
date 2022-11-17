#include <iostream>
#include "./Parsing/Joiner.hpp"
#include "./Parsing/Parser.hpp"

using namespace std;
//---------------------------------------------------------------------------
int main(int argc, char* argv[]) {

   Joiner joiner;
   // Read join relations
   string line;

   while (getline(cin, line)) {
      if (line == "Done") break;
      joiner.addRelation(line.c_str());
   }

   cout << "No more Relations!" << endl;

   // Preparation phase (not timed)
   // Build histograms, indexes,...

   QueryInfo i;
   line = "3 0 1|0.2=1.0&0.1=2.0&0.2=0.1|1.2 0.1";
   i.parseQuery(line);
   // while (getline(cin, line)) {
   //    cout << line<<endl;
   //    if (line == "F") continue; // End of a batch
   //    i.parseQuery(line);
   //    //cout << joiner.join(i);
   // }
   //cout << i.predicates[1].left<<endl;
   //cout << i.predicates[1].right<<endl;
   //cout << i.predicates[0].left.relId<<endl;
   //cout << i.predicates[0].right.relId<<endl;
   cout << i.filters[0].filterColumn.relId<<endl;

   //cout << i.filters[1].constant<<endl;

   return 0;
}
