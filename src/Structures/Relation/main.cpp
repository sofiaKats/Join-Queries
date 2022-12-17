#include <iostream>
#include "Relation.hpp"

using namespace std;

int main(void) {
    // ./workloads/small/r0
    Relation* relation = new Relation("../../main/workloads/small/r0", 0);
    relation->~Relation();
    exit(1);
    return 0;
}