#include "JoinEnum.hpp"

#include <set>
#include <string>

//-------------------------Set---------------------------------------

Set::Set(){
    prdcts = new Predicates*[10];
    this->setSize = 0;
}

Set::Set(Set* set, Predicates* p){
    setSize = 0;
    prdcts = new Predicates*[set->setSize + 1];
    for (int i = 0; i < set->setSize; i++){
        prdcts[i] = set->prdcts[i];
        setSize++;
    }
    prdcts[setSize++] = p;
}

void Set::print(){
    cout << "------Subset is: " << endl;
    //setSize = 1;
    // cout << setSize;
    // for (int i = 0; i < setSize; i++){
    //     cout << prdcts[i]->relation_left << " ";
    // }
    cout << endl;
}

//-------------------------SetArr---------------------------------------


SetArr::SetArr(){
    sets = new Set*[20]; 
    setArrSize = 0;   
}

//-------------------------JoinEnum---------------------------------------


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
        //cout << "here!" << endl;
        subsets->sets[subsetsIndex] = new Set();
        for (int j = 0; j < r; j++){      
            //cout << data[j]->binding_left << endl;      
            subsets->sets[subsetsIndex]->prdcts[j] = data[j];
            cout << "--" << data[j]->binding_left << " ";
            subsets->sets[subsetsIndex]->setSize++;
        }
        cout << endl;
        subsetsIndex++;
        subsets->setArrSize++;        
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
    //cout << "END" << endl;
    return subsets;
}

bool JoinEnum::subsetContains(Predicates* rel, Set* set){
    for (int i = 0; i < set->setSize; i++){
        if (equalPredicates(rel, set->prdcts[i])) return true;
    }
    cout << "false;" << endl;
    return false;
}

bool JoinEnum::equalPredicates(Predicates* p1, Predicates* p2){
    if (p1->binding_left == p2->binding_left && p1->binding_right == p2->binding_right 
        && p1->operation == p2->operation)
        return true;
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
            //S->print();
            for (int r = 0; r < relSetSize; r++){
                if (subsetContains(relSet[r], S)) continue;
                cout << "doesn't contain " << relSet[r]->operation << " " << relSet[r]->relation_left << r << endl;
                
                JoinTree* CurrTree = new JoinTree(S->prdcts, S->setSize, relSet[r]);
                Set* S_new = new Set(S, relSet[r]);

                JoinTreeNode* JoinTreeNode = bt->bestTrees[i]->contains(S_new->prdcts, S_new->setSize);
                // if ( JoinTreeNode == NULL || JoinTreeNode->jt->cost->cost() > CurrTree->cost->cost()){
                //     bt->bestTrees[i]->replace(JoinTreeNode, CurrTree);                    
                // }                
            }
        }
    }
    //return bt->bestTrees[relSetSize - 1]->getHead()->jt;
}