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
    if (table_size < 40)  {
        H = table_size;
    }
    else                        H = 40;

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

int Hashtable::find_empty_index(int i, int key){
    int j=-1; // linear search of array for empty space
    for(int bucket=i; bucket<table_size; bucket++)
    {
        if(hashtable[bucket]->get_has_value() == false){
            j = bucket;
            if (key==3682) cout << "j in find pos is " << j << endl;
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

void Hashtable::add_value(int pos, int value, int hash_value, Tuple* tuple){
    if (tuple->key == 3682) {cout << "-----------------------------------pos, hash_value is, table size is " << pos << " " << hash(tuple->payload) << " " << table_size << endl;}
    //hash_value = hash(tuple->payload);
    
    hashtable[pos]->set_value(tuple->key);
    hashtable[pos]->set_has_value(true);

    hashtable[pos]->setTuple(tuple);

    //update bitmaps
    int indx;
    int loop = 0;
    for (int i = pos; i > pos - H; i--){
        indx = (i + table_size) % table_size;
        if (hash_value == indx) {
            hashtable[indx]->set_bitmap_index_to_1(loop);
            //if (tuple->key == 470) {cout << "<><><><><>pos is " << i << endl;}
        }
        else hashtable[indx]->set_bitmap_index_to_0(loop);
        loop++;
    }
    if (tuple->key == 3682) cout << "value is " << value << endl;
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
        delete hashtable_old[i];
    }
    delete [] hashtable_old;
}


bool Hashtable::checkHashtableFull(){
    return (emptySpaces == 0);
}

bool Hashtable::checkBitmapFull(int index){
    return hashtable[index]->is_bitmap_full();
}

void Hashtable::add(int payload, int value){
    Tuple* tuple = new Tuple(value, payload);

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

bool Hashtable::insert(int hashed_payload, int value, Tuple* tuple){
    if (tuple->key == 3682) cout << "hashedh [ayload is isert" << hashed_payload << endl;
    int pos = findPos(hashed_payload, tuple->key);
    if (tuple->key == 3682) cout << "POS IS " << pos << endl;
    if (pos == -1) return false;

    add_value(pos, value, hashed_payload, tuple);
    emptySpaces--;
    return true;
}

int Hashtable::findPos(int hashed_payload, int key){
    int emptyPos = find_empty_index(hashed_payload, key);
    if (key == 3682) cout << "empty Pos is is findPos" << emptyPos << endl;

    return slideLeft(hashed_payload, emptyPos);
}

int Hashtable::slideLeft(int hashed_payload, int emptyPos){
    while( ((emptyPos-hashed_payload + table_size) % table_size) >= H ) {
        emptyPos = findSwapNeighbourPos(emptyPos);
        if (emptyPos == -1) return emptyPos;
    }
    return emptyPos;
}

int Hashtable::swapEmpty(int emptyPos, int swapNeighborPos, int value, int hashed_payload, Tuple* tuple){
    //if (tuple->key == 999) cout << "hashedh [ayload is " << hashed_payload << endl;
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
    int swapNeighborPos = -1;
    int posLeftToCheckBitmaps = H-1;
    int bucket = (emptyPos - (H-1) + table_size) % table_size;
    bool changed = false;
    //checking each neighboring bucket's bitmap to find y's original hash value (k)
    for (int i = 0; i< H - 1; i++){

        swapNeighborPos = checkBucketBitmap(bucket, swapNeighborPos, changed, posLeftToCheckBitmaps);

        if (swapNeighborPos!=-1){
            emptyPos = swapEmpty(emptyPos, swapNeighborPos, hashtable[swapNeighborPos]->getTuple()->key, hash(hashtable[swapNeighborPos]->getTuple()->payload), hashtable[swapNeighborPos]->getTuple());
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

Matches* Hashtable::contains(Tuple* tuple){
    //find hash value and neighborhood
    int nei = H;
    Matches* matches = new Matches(nei);
    int payload2 = tuple->payload;
    int hashhop = hash(payload2);

    // if (tuple->payload == 5160 && tuple->key==198) {
    //     cout << "hashhop is " << hashhop << endl;
    //     for (int i = 0; i < nei; i++){
    //         if ( hashtable[hashhop+i]->get_has_value()) cout << hashtable[hashhop+i]->getTuple()->key << " ";
    //         else cout << "0 "; 
    //     }
    //     hashtable[hashhop]->print_bitmap();
    // }    
    int currentBucket = hashhop;

    for (int loops = 0; loops < nei; loops++){
        if (hashtable[hashhop]->get_bitmap_index(loops) == 1){

            // if (tuple->payload == 3233){
            //     cout << "hashtable_cuur " << matches->activeSize << endl;
            // }

            int payload1 = hashtable[currentBucket]->getTuple()->payload;
            // if (tuple->payload == 3233){
            //     cout << "hashtable_cuur " << hashtable[currentBucket]->getTuple()->key << endl;
            // }

            if (payload1 == payload2){
                matches->tuples[matches->activeSize] = new Tuple(hashtable[currentBucket]->getTuple()->key, tuple->key);
                matches->activeSize++;
            }
        }
        currentBucket = findNeighborPosByK(currentBucket, 1);
    }

    return matches;
}
