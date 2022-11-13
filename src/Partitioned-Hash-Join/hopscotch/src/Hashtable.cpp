#include <iostream>
#include <cstdlib>
#include <cstring>
#include "math.h"
#include "Hashtable.h"
using namespace std;

Hashtable::Hashtable(int tableR_size){
    this->depth = findClosestPowerOf2(tableR_size);
    this->table_size = pow(2,depth);
    this->emptySpaces = table_size;
    if (table_size<8)  {
        H = table_size;
    }
    else                        H = 8;

    hashtable = new Index*[table_size];
    for (int i=0; i<table_size; i++)
        hashtable[i] = new Index(H);

    //cout << "Hashtable size is " << table_size << " tableR size " << tableR_size << endl;
}

Hashtable::~Hashtable(){
    for (int i = 0; i < table_size; i++){
        delete hashtable[i];
    }
    delete[] hashtable;
}

int Hashtable::findClosestPowerOf2(int number){
    int counter = 0;
    int power = 1;
    while(power < number){
        power*=2;
        counter++;
    }

    return counter;
}

int Hashtable::hash(int id){

    int i = 2;
    int j = 32;
    int hashed_value = (int) (id*2654435761 % (int)pow(i,j));

    hashed_value = hashed_value >> (31-depth);
    return hashed_value;
}

void Hashtable::print_hashtable() {
    cout << "FINAL HASHTABLE: " << endl;
    for(int bucket=0; bucket<table_size; bucket++) {
        if (hashtable[bucket]->get_has_value()) cout << "   " << hashtable[bucket]->get_value();
        else cout << "   " << 0;
    }

    cout << endl << "\n Corresponding bitmaps: " << endl;
    for(int bucket = 0; bucket < table_size; bucket++) {
        cout << "BUCKET " << bucket << " with value:  ";
        if (hashtable[bucket]->get_has_value()) cout <<  hashtable[bucket]->get_value() << " : ";
        else                                    cout << "0: ";

        for (int bit=0; bit< H ; bit++)  cout << "  " << hashtable[bucket]->get_bitmap_index(bit);
        cout << endl;
    }
}

int Hashtable::find_empty_index(int i){
    int j=-1; // linear search of array for empty space
    for(int bucket=i; bucket<table_size; bucket++)
    {
        if(hashtable[bucket]->get_has_value() == false){
            j = bucket;
            break;
        }
    }
    if (j==-1){
        //this means that from our point to the end of the table on the right, there are no free spots
        // but since we treat this hashtable as a cycle, we also check from the start of the table until tehe position i-1
        for(int bucket=0; bucket<i; bucket++)
        {
            if(hashtable[bucket]->get_has_value() == false){
                j = bucket;
                break;
            }
        }
    }
    return j;
}

void Hashtable::add_value(int pos, int value, int hash_value, Tuple2* tuple){
    hashtable[pos]->set_value(value);
    hashtable[pos]->set_has_value(true);

    hashtable[pos]->setTuple(tuple); 

    //update bitmaps
    int indx;
    int loop = 0;
    for (int i = pos; i > pos- H; i--){
        indx = (i+table_size) %table_size;
        if (hash_value == indx) {
            hashtable[indx]->set_bitmap_index_to_1(loop);
        }
        else hashtable[indx]->set_bitmap_index_to_0(loop);
        loop++;
    }
}

void Hashtable::remove_value(int pos, int hash_value ){
    //update bitmaps
    int loop = 0;
    int indx;
    for (int i = pos; i > pos- H; i--){
        indx = (i+table_size) %table_size;
        if (hash_value == indx) {
            hashtable[indx]->set_bitmap_index_to_0(loop);
        }
        loop++;
    }
    hashtable[pos]->set_value(0);
    hashtable[pos]->set_has_value(false);
}


void Hashtable::resize(){
    //cout << "start resize " << endl;
    Index** hashtable_old = hashtable;
    int table_size_old = table_size;

    this->table_size = table_size*2;
    this->depth+=1;

    this->hashtable = new Index*[this->table_size];
    for (int i=0; i<this->table_size; i++)
        this->hashtable[i] = new Index(H);

    this->emptySpaces = this->table_size;

    //re-entering the previous elements
    for (int i=0; i < table_size_old; i++){
        if (hashtable_old[i]->get_has_value())  {
            add(hashtable_old[i]->getTuple()->payload, hashtable_old[i]->getTuple()->key);
        }
    }

    for (int i=0; i<table_size_old; i++) delete hashtable_old[i];
    delete [] hashtable_old;
}


bool Hashtable::checkHashtableFull(){
    return (emptySpaces == 0);
}

bool Hashtable::checkBitmapFull(int index){
    return hashtable[index]->is_bitmap_full();
}

void Hashtable::add(int payload, int value){
    Tuple2* tuple = new Tuple2(value, payload);


    if (checkHashtableFull()) resize();
    int hashed_payload = hash(payload);

    while (checkBitmapFull(hashed_payload)) {
        //cout << "!!!!!!!!!!!!!!Resize2" << endl;
        resize();
        hashed_payload = hash(payload);
    }

    while (!insert(hashed_payload, value, tuple)){
        //cout << "!!!!!!!!!!!!!!resize3" << endl;
        resize();
        hashed_payload = hash(payload);
    }
}

bool Hashtable::insert(int hashed_payload, int value, Tuple2* tuple){
    int pos = findPos(hashed_payload);
    if (pos == -1) return false;

    add_value(pos, value, hashed_payload, tuple);
    emptySpaces--;
    return true;
}

int Hashtable::findPos(int hashed_payload){
    int emptyPos = find_empty_index(hashed_payload);
    return slideLeft(hashed_payload, emptyPos);
}

int Hashtable::slideLeft(int hashed_payload, int emptyPos){
    while( ((emptyPos-hashed_payload) % table_size) >= H ) {
        emptyPos = findSwapNeighbourPos(emptyPos);
        if (emptyPos == -1) return emptyPos;
    }
    return emptyPos;
}

int Hashtable::swapEmpty(int emptyPos, int swapNeighborPos, int value, int hashed_payload, Tuple2* tuple){
    add_value(emptyPos, value, hashed_payload, tuple);
    remove_value(swapNeighborPos, hashed_payload);
    emptyPos = swapNeighborPos;
    return emptyPos;
}

int Hashtable::checkBucketBitmap(int bucket, int& swapNeighborPos, bool& changed, int loops){
    for (int bit_pos = 0; bit_pos < loops; bit_pos++){
        if(hashtable[bucket]->get_bitmap_index(bit_pos) == 1){
            changed = true;
            swapNeighborPos = findNeighborPosByK(bucket, bit_pos);
            break;
        }
    }
    return swapNeighborPos;
}


int Hashtable::findSwapNeighbourPos(int emptyPos){
    int swapNeighborPos=-1;
    int posLeftToCheckBitmaps = H-1;
    int bucket = emptyPos - (H-1);
    bool changed = false;
    //checking each neighboring bucket's bitmap to find y's original hash value (k)
    for (int i = 0; i< H - 1; i++){

        swapNeighborPos = checkBucketBitmap(bucket, swapNeighborPos, changed, posLeftToCheckBitmaps);

        if (swapNeighborPos!=-1){
            emptyPos = swapEmpty(emptyPos, swapNeighborPos, hashtable[swapNeighborPos]->get_value(), hash(hashtable[swapNeighborPos]->get_value()), hashtable[swapNeighborPos]->getTuple());
            break;
        }
        bucket = findNeighborPosByK(bucket, 1);
        posLeftToCheckBitmaps--;
    }
    if (!changed) {
        //cout << "No element y, table need rehashing!" << endl;
        return -1;
    }
    return emptyPos;
}


int Hashtable::findNeighborPosByK(int currPos, int k){
    return (currPos + k + table_size)%table_size;
}

void Hashtable::contains(Tuple2* tuple){
    //find hash value and neighborhood
    int nei = H;
    int payload2 = tuple->payload;
    int hashhop = hash(payload2);

    int currentBucket = hashhop;

    for (int loops = 0; loops < nei ; loops++){
        if (hashtable[hashhop]->get_bitmap_index(loops) == 1){
        int payload1 = hashtable[currentBucket]->getTuple()->payload;

        if (payload1 == payload2){
            cout << "------- Match: " << payload2 << " RowId R: " << hashtable[currentBucket]->getTuple()->key << " RowId S: " << tuple->key << " -------\n";
        }
        }
        currentBucket = findNeighborPosByK(currentBucket, 1);
    }
}