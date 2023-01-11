#include "BestTree.hpp"
#include <set>

typedef struct Set{
    Predicates** prdcts;
    int setSize; 
    Set();  
    Set(Set*, Predicates*);
    void print();
}Set;

typedef struct SetArr{
    Set** sets;
    int setArrSize;
    SetArr();  
    void add(Set*);  
}SetArr;

class JoinEnum{
private:
    BestTree* bt;
    Predicates** relSet; //all predicates of query   
    int relSetSize;
    int filterIndex;    //this is the index from which the non-filter predicates start on the 
                        //relset array, while the filters are on the start
    //
    SetArr* getSubsets(int);
    void getSubsetsUtil(int, int, Predicates**, int, SetArr*, int);
    bool subsetContains(Predicates*, Set*);
    bool equalPredicates(Predicates*, Predicates*);
    bool connected(Predicates*, Set*);
public:
    JoinEnum(Query*, Relation**, int);
    JoinTree* DP_linear();

};