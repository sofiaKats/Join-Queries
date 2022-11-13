#include <iostream>
#include <cstring>
#include "Index.h"

using namespace std;

class Hashtable{
private:
    int emptySpaces;
    int table_size;
    int depth;
    int H;
    Index** hashtable;

    int hash(int);
    int findNeighborPosByK(int, int);
    bool checkHashtableFull();
    bool checkBitmapFull(int);
    bool insert(int, int, Tuple2*);
    int findPos(int);
    int checkBucketBitmap(int, int&, bool&, int);
    int swapEmpty(int, int, int, int, Tuple2*);
    int findSwapNeighbourPos(int);
    int slideLeft(int, int);
    int find_empty_index(int);
    void add_value(int, int, int, Tuple2*);
    void remove_value(int, int);
    void resize();
    int findClosestPowerOf2(int);
public:
    Hashtable(int);
    ~Hashtable();
    void add(int, int);
    void print_hashtable();
    void contains(Tuple2*); 
};
