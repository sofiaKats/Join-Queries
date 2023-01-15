//#include "../Parser/parser.h"
#include "Cost.hpp"

class JoinTree{
public:
    Predicates** arr;
    int size;

    Cost* cost;

    JoinTree(Predicates**,  int, Predicates*, Cost*);
    JoinTree(Predicates*, Relation**, int);
    ~JoinTree();
    void print();
};


typedef struct JoinTreeNode{
    JoinTree* jt;
    int index;
    JoinTreeNode* next;
    JoinTreeNode(JoinTree*,int);
    ~JoinTreeNode();
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
