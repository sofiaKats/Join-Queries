#include <iostream>
#include "./Joiner/Joiner.hpp"
#include "./Parsing/Parser.hpp"

using namespace std;
//---------------------------------------------------------------------------
int main(int argc, char* argv[]) {

   Joiner joiner;
   string line;

   cout << ">>> Insert Relations:" << endl;

   const string workspace = "./workloads/small/";

   try{
     while (getline(cin, line)) {
        if (line == "Done") break;
        try{
          joiner.AddRelation((workspace + line).c_str());
        }
        catch(const exception& e){
          cout << e.what() << endl;
        }
     }

     // Preparation phase (not timed)
     // Build histograms, indexes,...

     cout << ">>> Insert Queries:" << endl;

     QueryInfo i;
     line = "3 0 1|0.2=1.0&0.1=2.0&0.2>3000|1.2 0.1";
     i.parseQuery(line);

     string results = joiner.Join(i);

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

     return 0;
   }
   catch (const exception& e){
     cout << e.what() << endl;
     return 1;
   }
}
