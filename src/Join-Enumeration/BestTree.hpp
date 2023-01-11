#include <iostream>
#include <cstdlib>
#include <cstring>

#include "JoinTreeList.hpp"


class BestTree{
private:
public:
    JoinTreeList** bestTrees;
    Relation** rels;
    int relSize;
    int size;
    int activeSize;

    BestTree(int, Relation**, int);
    ~BestTree();
    void print();
};

