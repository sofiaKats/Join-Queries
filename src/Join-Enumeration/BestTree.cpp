#include "BestTree.hpp"

#include <iostream>

using namespace std;

BestTree::BestTree(int N){
    size = N;
    activeSize = 0;
    bestTrees = new JoinTreeList*[N];
    for (int i = 0; i < N; i++){
        bestTrees[i] = new JoinTreeList();
    }
}

void BestTree::print(){
    for (int i = 0; i < size; i++){
        cout << "BestTree with subset size: " << i << endl;
        bestTrees[i]->print();
        cout << endl;
    }
}
