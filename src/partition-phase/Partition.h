#include "../Structures/Structures.h"
#include "../Multithreading/JobScheduler.h"
#include <math.h>

class Partition {
private:
  int n; //for number of lsb for hashing
  int startIndex;
  int endIndex;
  int largestTableSize = 0;
  RelColumn* rel;
  uint32_t Hash(uint32_t, int);
  Hist* CreateHistogram();
  PrefixSum* CreatePrefixSum(Hist*);
  static void* thread_CreateHistogram(void*);
  static void* thread_BuildPartitionedTable(void*);

public:
  Partition(RelColumn*, int, int = 0, int = -1);
  Part* BuildPartitionedTable();
  uint32_t GetLargestTableSize();
};

typedef struct HistArgs{
  Partition* instance;
  Hist** histArr;
  int thread_id;
  uint32_t start, end;
  HistArgs(Partition* obj, Hist** h, int id, uint32_t s, uint32_t e):
    instance(obj), histArr(h), thread_id(id), start(s), end(e){}
}HistArgs;

typedef struct BuildArgs{
  Partition* instance;
  Part* part;
  int thread_id;
  uint32_t start, end;
  BuildArgs(Partition* obj, Part* p, int id, uint32_t s, uint32_t e):
    instance(obj), part(p), thread_id(id), start(s), end(e){}
}BuildArgs;
