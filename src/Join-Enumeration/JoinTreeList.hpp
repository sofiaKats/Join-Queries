//#include "../Parser/parser.h"
#include "Cost.hpp"

class JoinTree{
public:
    Predicates** arr;
    int size;

    Cost* cost;

    JoinTree(Predicates**,  int, Predicates*);
    JoinTree(Predicates*);
    void print();
};


typedef struct JoinTreeNode{
    JoinTree* jt;
    //bitVector of relationships
    int index;
    JoinTreeNode* next;
    JoinTreeNode(JoinTree*,int);
}JoinTreeNode;

class JoinTreeList
{
private:
    JoinTreeNode* head;
    int size;

    bool equalPredicates(Predicates*, Predicates*);
public:
    JoinTreeList();
    ~JoinTreeList();
    void add(JoinTree*);
    JoinTreeNode* contains(Predicates**, int);
    void replace(JoinTreeNode*, JoinTree*);
    JoinTreeNode* getHead();
    void print();
};