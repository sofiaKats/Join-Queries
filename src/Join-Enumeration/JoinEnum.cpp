#include "JoinEnum.hpp"

#include <set>
#include <string>

Set::Set(){
    prdcts = new Predicates*[10];
}

Set::Set(Set* set, Predicates* p){
    prdcts = new Predicates*[set->setSize];
    for (int i = 0; i < set->setSize; i++){
        prdcts[i] = set->prdcts[i];
        setSize++;
    }
    prdcts[setSize] = p;
    setSize;
}

SetArr::SetArr(){
    sets = new Set*[20];    
}

JoinEnum::JoinEnum(Query* q){
    relSet = new Predicates*[q->number_of_predicates];
    for (int i = 0; i < q->number_of_predicates; i++){
        relSet[i] = new Predicates();
        relSet[i] = q->prdcts[i];
    }
    relSetSize = q->number_of_predicates;
    bt = new BestTree(relSetSize);
}

void JoinEnum::getSubsetsUtil(int r, int index, Predicates** data, int i, SetArr* subsets, int subsetsIndex){
    int n = relSetSize;
    // Current combination is ready, print it
    if (index == r) {
        subsets->sets[subsetsIndex] = new Set();
        subsets->sets[subsetsIndex]->prdcts = data;
        subsets->sets[subsetsIndex]->setSize++;

        subsetsIndex++;
        subsets->sets++;        
        return;
    }
 
    // When no more elements are there to put in data[]
    if (i >= n)
        return;
 
    // current is included, put next at next location
    data[index] = relSet[i];
    getSubsetsUtil(r, index + 1, data, i + 1, subsets, subsetsIndex);
 
    // current is excluded, replace it with next
    // (Note that i+1 is passed, but index is not
    // changed)
    getSubsetsUtil(r, index, data, i + 1, subsets, subsetsIndex);

}

SetArr* JoinEnum::getSubsets(int r){
    // A temporary array to store all combination
    // one by one
    Predicates* data[r];

    SetArr* subsets = new SetArr();
 
    // Print all combination using temporary array 'data[]'
    getSubsetsUtil(r, 0, data, 0, subsets, 0);
    return subsets;
}

bool JoinEnum::subsetContains(Predicates* rel, Set* set){
    for (int i = 0; i < set->setSize; i++){
        if (rel == set->prdcts[i]) return true;
    }
    return false;
}

JoinTree* JoinEnum::DP_linear(){
    for (int i = 0; i < relSetSize; i++){
        JoinTree* jt = new JoinTree(relSet[i]);
        bt->bestTrees[0]->add(jt);
    }
    bt->print();
    for (int i = 1; i < relSetSize; i++){
        SetArr* subsets = getSubsets(i);
        for (int j = 0; j < subsets->setArrSize; j++){
            Set* S = subsets->sets[j];
            // for (int r = 0; r < relSetSize; r++){
            //     if (subsetContains(relSet[r], S)) continue;
                
            //     JoinTree* CurrTree = new JoinTree(S->prdcts, S->setSize, relSet[r]);
            //     Set* S_new = new Set(S, relSet[r]);

            //     JoinTreeNode* JoinTreeNode = bt->bestTrees[i-1]->contains(S_new->prdcts, S_new->setSize);
            //     if ( JoinTreeNode == NULL || JoinTreeNode->jt->cost->cost() > CurrTree->cost->cost()){
            //         bt->bestTrees[i-1]->replace(JoinTreeNode, CurrTree);                    
            //     }                
            // }
        }
    }
    return bt->bestTrees[relSetSize - 1]->getHead()->jt;
}