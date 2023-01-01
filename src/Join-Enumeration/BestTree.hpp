#include <iostream>
#include <cstdlib>
#include <cstring>

#include "JoinTreeList.hpp"

typedef struct Node{
    char combination[100];
}Node;


class BestTree{
private:
public:
    JoinTreeList** bestTrees;
    int size;

    BestTree(int);
    ~BestTree();
    void print();
};

