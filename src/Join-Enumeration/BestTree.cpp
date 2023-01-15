#include "BestTree.hpp"

#include <iostream>

using namespace std;

BestTree::BestTree(int N, Relation** rels, int relSize){
    size = N;
    activeSize = 0;
    bestTrees = new JoinTreeList*[N];
    for (int i = 0; i < N; i++){
        bestTrees[i] = new JoinTreeList();
    }
    this->rels = rels;
    this->relSize = relSize;

}

BestTree::~BestTree(){
    for (int i = 0; i < size; i++){
        delete bestTrees[i];
    }
    delete [] bestTrees;
}

void BestTree::print(){
    for (int i = 0; i < size; i++){
        cout << "BestTree with subset size: " << i << endl;
        bestTrees[i]->print();
        cout << endl;
    }
}
