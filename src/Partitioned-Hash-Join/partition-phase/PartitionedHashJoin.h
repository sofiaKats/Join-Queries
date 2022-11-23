#include "Partition.h"

#define MAX_PASSES 2
#define N 2

class PartitionedHashJoin {
private:
  Relation* relR;
  Relation* relS;
  RowIds* rowsR;
  RowIds* rowsS;

  void Merge(Part*, Part*, int, int);
  int ExistsInPrefix(int, PrefixSum*);

public:
  PartitionedHashJoin(Relation& RowIds& ,Relation&, RowIds&);
  void Solve();
  int PartitionRec(Part*, Column*, int = MAX_PASSES, int = N, int = 0, int = 0, int = -1);
  void BuildHashtables(Part*);
  void Join(Part*, Part*);
  void PrintHashtables(Part*);
  void PrintRelation(Column*);
  void PrintPrefix(PrefixSum*);
  void PrintPart(Part*, bool = false);
};
