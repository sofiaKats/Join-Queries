#include <iostream>
#include <cstring>
using namespace std;


typedef struct Tuple2{
    int key;
    int payload;
    Tuple2(int key, int payload){
        this->key = key;
        this->payload = payload;
    }
}Tuple2;

// each index of the hopscoth table has a value and a corresponding bitmap
class Index
{
private:
    int value;
    bool has_value; //flag to check if position is empty
    int* bitmap;
    int H;
    Tuple2* tuple;
public:
    Index(int);
    ~Index();
    void set_bitmap_index_to_1(const int index);
    void set_bitmap_index_to_0(const int index);
    int  get_bitmap_index(const int index);
    void print_bitmap(void);
    bool get_has_value(void);
    void set_has_value(bool flag);
    Tuple2* getTuple();
    void setTuple(Tuple2*);
    void set_value(const int val);
    int  get_value(void);
    bool is_bitmap_full(); //returns true if full, false if not full
};

int temp_find_hash(int, int**);
