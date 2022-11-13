#include "Structures.h"
#include <math.h>

class Partition {
private:
  int n; //for number of lsb for hashing
  int startIndex;
  int endIndex;
  int largestTableSize = 0;
  Column* rel;
  int Hash(int32_t, int);
  Hist* CreateHistogram();
  PrefixSum* CreatePrefixSum(Hist*);

public:
  Partition(Column*, int, int = 0, int = -1);
  Part* BuildPartitionedTable();
  uint32_t GetLargestTableSize();
};
