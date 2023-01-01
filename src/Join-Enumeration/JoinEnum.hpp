#include "BestTree.hpp"
#include <set>

typedef struct Set{
    Predicates** prdcts;
    int setSize = 0; 
    Set();  
    Set(Set*, Predicates*);
}Set;

typedef struct SetArr{
    Set** sets;
    int setArrSize = 0;
    SetArr();    
}SetArr;

class JoinEnum{
private:
    BestTree* bt;
    Predicates** relSet;
    int relSetSize;
    //
    SetArr* getSubsets(int);
    void getSubsetsUtil(int, int, Predicates**, int, SetArr*, int);
    bool subsetContains(Predicates*, Set*);
public:
    JoinEnum(Query*);
    JoinTree* DP_linear();

};