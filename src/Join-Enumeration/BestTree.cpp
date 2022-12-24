#include "BestTree.hpp"

#include <iostream>

using namespace std;

int temp_cnt = 0;

BestTree::BestTree(int N){
    size = factorial(N);
    cout << "Size is " << size << endl;
    combs = new Node*[size];
    for (int i = 0; i < size; i++){
        combs[i] = new Node();
    }
}

int BestTree::factorial(int N){
    int fact = 1;
    for(int x = 1; x <= N; x++)
        fact = fact*x;
    return fact;
}

int BestTree::hash(char* str){
    temp_cnt++;
    return temp_cnt - 1;
}

void BestTree::add(char* prdcts_set){
    int index = hash(prdcts_set);
    strcpy(combs[index]->combination, prdcts_set);
}