#include <iostream>
#include <cstdlib>
#include <cstring>

typedef struct Node{
    char combination[100];
}Node;


class BestTree{
private:
    Node** combs;
    int size;
    int hash(char*);
    int factorial(int);
public:
    BestTree(int);
    ~BestTree();
    void add(char*);
    int contains();
    int cost(char*);
};

