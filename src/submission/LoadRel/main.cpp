#include <iostream>
#include "Relation.hpp"

using namespace std;

int main(int argc, char* argv[]){
    char* fileName = argv[1];
    Relation* rel = new Relation(fileName);
    for (uint64_t i=0;i<rel->columns.size();++i) {
        for (auto& c : rel->columns) {
            cout << c[i] << '|';
        }
        cout << "\n";
    }
}