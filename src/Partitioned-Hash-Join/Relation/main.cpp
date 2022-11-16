#include <iostream>
#include "Relation.hpp"

using namespace std;

int main(int argc, char* argv[]){
    char* fileName = argv[1];
    Relation* rel = new Relation(fileName);

    QueryInfo i;
    string line = "3 0 1|0.2=1.0&0.1=2.0&0.2>3499|1.2 0.1"
    i.parseQuery(line);


    /*while (getline(cin, line)) {
       if (line == "F") continue; // End of a batch
       i.parseQuery(line);
       cout << joiner.join(i);
    }*/

    /*Queries
    3 0 1|0.2=1.0&0.1=2.0&0.2>3499|1.2 0.1
    5 0|0.2=1.0&0.3=9881|1.1 0.2 1.0
    9 0 2|0.1=1.0&1.0=2.2&0.0>12472|1.0 0.3 0.4
    */
    return 0;
}
