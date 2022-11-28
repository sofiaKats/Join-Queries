#include "Partition.h"

#define MAX_PASSES 2
#define N 2

class PartitionedHashJoin {
private:
  RelColumn* relR;
  RelColumn* relS;
  void Merge(Part*, Part*, int, int);
  int ExistsInPrefix(int, PrefixSum*);

public:
  PartitionedHashJoin(RelColumn*, RelColumn*);
  void Solve();
  int PartitionRec(Part*, RelColumn*, int = MAX_PASSES, int = N, int = 0, int = 0, int = -1);
  void BuildHashtables(Part*);
  void Join(UsedRelations&, Part*, Part*);
  void PrintHashtables(Part*);
  void PrintRelation(RelColumn*);
  void PrintPrefix(PrefixSum*);
  void PrintPart(Part*, bool = false);
};
