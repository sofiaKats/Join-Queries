#include "Partition.h"

#define MAX_PASSES 2
#define N 2

class PartitionedHashJoin {
private:
  Column* relR;
  Column* relS;
  void Merge(Part*, Part*, int, int);
  int ExistsInPrefix(int, PrefixSum*);

public:
  PartitionedHashJoin(Column*, Column*);
  void Solve();
  int PartitionRec(Part*, Column*, int = MAX_PASSES, int = N, int = 0, int = 0, int = -1);
  void BuildHashtables(Part*);
  void Join(Part*, Part*);
  void PrintHashtables(Part*);
  void PrintRelation(Column*);
  void PrintPrefix(PrefixSum*);
  void PrintPart(Part*, bool = false);
};
