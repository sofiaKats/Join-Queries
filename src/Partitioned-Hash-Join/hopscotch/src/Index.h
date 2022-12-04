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

typedef struct Matches {
  Tuple** tuples;
  uint32_t size;
  uint32_t activeSize = 0;
  Matches(uint32_t size){
    this->size = size;
    tuples = new Tuple*[size]{};
  }
  ~Matches(){
    for (int i=0; i<size; i++)
      delete tuples[i];
    delete[] tuples;
  }
} Matches;

// each index of the hopscoth table has a value and a corresponding bitmap
class Index
{
private:
    int value;
    bool has_value; //flag to check if position is empty
    int* bitmap;
    int H;
    Tuple* tuple;
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
};

int temp_find_hash(int, int**);
