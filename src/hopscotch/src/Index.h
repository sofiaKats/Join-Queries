#include <iostream>
#include <cstring>
using namespace std;

typedef struct Tuple {
  int32_t key = 0;
  int32_t payload = 0;
  Tuple(){}
  Tuple(int32_t key, int32_t payload){
      this->key = key;
      this->payload = payload;
  }
} Tuple;

typedef struct MatchesPtr {
  Tuple** tuples;
  uint32_t size;
  uint32_t activeSize = 0;
  uint32_t joinSize = 0;

  MatchesPtr(uint32_t size){
    this->size = size;
    tuples = new Tuple*[size]{};
  }
  ~MatchesPtr(){
    delete[] tuples;
  }
} MatchesPtr;

class Duplicates{
public:
  int* arr;
  int size;
  int activeSize = 0;
  Duplicates(int size){
    this->size = size;
    this->arr = new int[size];
    //for (int i = 0; i < size; i++) {this->arr[i] = new int;}
  }
  ~Duplicates(){
    delete [] arr;
  }
  void resize(){
    delete [] arr;
    size*=2;
    arr = new int[size];
    //for (int i = 0; i < size; i++) {this->arr[i] = new int;}
  }
  bool isFull(){
    if (activeSize == size) return true;
    return false;
  }

};

// each index of the hopscoth table has a value and a corresponding bitmap
class Index
{
private:
    int value;
    bool has_value; //flag to check if position is empty
    int* bitmap;
    int H;
    Tuple* tuple;
    Duplicates* duplicates;
public:
    Index(int);
    ~Index();
    void set_bitmap_index_to_1(const int index);
    void set_bitmap_index_to_0(const int index);
    int  get_bitmap_index(const int index);
    void print_bitmap(void);
    bool get_has_value(void);
    void set_has_value(bool flag);
    Tuple* getTuple();
    void setTuple(Tuple*);
    void set_value(const int val);
    int  get_value(void);
    bool is_bitmap_full(); //returns true if full, false if not full
    bool has_duplicates();
    void addDupl(int);
    void print();
    Duplicates* getDuplicates();
    void setDuplicates(Duplicates*);
    bool searchDupls(int);
};
