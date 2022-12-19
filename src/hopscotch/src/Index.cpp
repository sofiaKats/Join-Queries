#include <iostream>
#include <cstdlib>
#include <cstring>
#include "Index.h"
using namespace std;

Index::Index(int H) : has_value(false)
{
    bitmap = new int[H]{0};
    // initializing bitmap with 0s
    for (int i = 0; i<H; i++){
        bitmap[i] = 0;
    }
    this->H = H;
    duplicates = nullptr;
    tuple = nullptr;
}

Index::~Index()
{
    delete [] bitmap;
    if (has_value)
      delete tuple;
}

void Index::set_bitmap_index_to_1(const int index) { bitmap[index] = 1;}

void Index::set_bitmap_index_to_0(const int index) { bitmap[index] = 0; }

int Index::get_bitmap_index(const int index) {return bitmap[index]; }

bool Index::get_has_value(void) { return has_value; }

void Index::set_has_value(bool flag) { has_value = flag;}

void Index::set_value(const int val) { value = val; }

int  Index::get_value(void) { return value;}

void Index::print_bitmap(void) {
    for(int i=0; i<H; i++) cout << bitmap[i] ;
    cout << endl;
}

//returns true if full, false if not full
bool Index::is_bitmap_full() {
    for(int i = 0; i < H; i++)
        if(bitmap[i] == 0) return false;
    return true;
}


Tuple* Index::getTuple(){ return tuple;}

void Index::setTuple(Tuple* t) {this->tuple = t;}

bool Index::has_duplicates(){
    if (duplicates == nullptr) return false; 
    else return true;
}

void Index::addDupl(int value){
    if (duplicates == nullptr){
        duplicates = new Duplicates(100);
    }
    if (duplicates->isFull()) duplicates->resize();
    duplicates->arr[duplicates->activeSize] = value;
    duplicates->activeSize++;
}

void Index::print(){
    cout << "Printing duplicatess!" << endl;
    if (duplicates == nullptr) return;
    for (int i = 0; i < duplicates->activeSize; i++){
        cout << " " << duplicates->arr[i];
    }
    cout << endl;
}

Duplicates* Index::getDuplicates(){
    return duplicates;
}

void Index::setDuplicates(Duplicates* dupl){
    this->duplicates = dupl;
}

bool Index::searchDupls(int key){
    if (duplicates == nullptr) return false;
    for (int i = 0; i < duplicates->activeSize; i++){
        if (duplicates->arr[i] == key) return true;
    }
    return false;
}