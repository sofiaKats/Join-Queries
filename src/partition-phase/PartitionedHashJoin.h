#include "Partition.h"

#define MAX_PASSES 2
#define N 2

class PartitionedHashJoin {
private:
  RelColumn* relR;
  RelColumn* relS;
  void Merge(Part*, Part*, int, int);
  int ExistsInPrefix(int, PrefixSum*);
  static void* thread_Join(void* vargp);

public:
  PartitionedHashJoin(RelColumn*, RelColumn*);
  Matches* Solve();
  int PartitionRec(Part*, RelColumn*, int = MAX_PASSES, int = N, int = 0, int = 0, int = -1);
  void BuildHashtables(Part*);
  Matches* Join(Part*, Part*);
  void PrintHashtables(Part*);
  void PrintRelation(RelColumn*);
  void PrintPrefix(PrefixSum*);
  void PrintPart(Part*, bool = false);
};

typedef struct JoinArgs{
  Matches *matches;
  Part *part1, *part2;
  uint32_t i, hashi;
  pthread_mutex_t *lock;
  JoinArgs(Matches* final, Part* p1, Part* p2, uint32_t hashIndex, uint32_t s, pthread_mutex_t* mtx):
    matches(final), part1(p1), part2(p2), hashi(hashIndex), i(s), lock(mtx){}
}JoinArgs;
