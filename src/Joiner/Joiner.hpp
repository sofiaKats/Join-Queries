#pragma once
#include "../partition-phase/PartitionedHashJoin.h"
#include "../Structures/Structures.h"
#include "../Parser/parser.h"
#include "../Join-Enumeration/JoinEnum.hpp"

//---------------------------------------------------------------------------
class Joiner {
private:
  void updateUsedRelations(UsedRelations*, Matches*, int, int);
  void updateURFirst(UsedRelations*, Matches*, int, int);
  void updateURonlyR(UsedRelations*, Matches*, int, int);
  void updateURonlyS(UsedRelations*, Matches*, int, int);
  void updateURfilter(UsedRelations*, int, SingleCol*);
  void updateURself(UsedRelations*, int,int, SelfCols*);

  bool isSelfJoin(unsigned int, unsigned int);
  bool isFilterJoin(char);

  //SingleCol* selfJoin(RelColumn*, RelColumn*);
  SelfCols* selfJoin(RelColumn*, RelColumn*);
  SingleCol* filterJoin(RelColumn*, char, int);

  MatchRow* getFirstURrow(UsedRelations*);

  void moveUR(UsedRelations*, UsedRelationsTemp*);
  void tempStoreDuplicatesR(UsedRelations*, int, UsedRelationsTemp*, int, Matches*, uint32_t, int);
  void tempStoreDuplicatesS(UsedRelations*, int, UsedRelationsTemp*, int, Matches*, uint32_t, int);

  void printMatches(Matches* matches);
  bool bothRelsUsed(UsedRelations*, int, int);

public:
  /// The relations that might be joined
  Relation** relations;
  uint32_t numRelations = 0;
  /// Add relation
  void AddRelation(const char* fileName);
  /// Get relation
  Relation& GetRelation(unsigned id);
  /// Get relation column
  RelColumn* GetRelationCol(unsigned, unsigned);
  /// Get relation column from filtered relation
  RelColumn* GetUsedRelation(UsedRelations*, unsigned, unsigned, unsigned);
  /// Joins a given set of relations
  static void* thread_executeQuery(void*);
  /// Prints used relations table
  void PrintUsedRelations(UsedRelations*);
  /// Checksum
  uint64_t Checksum(UsedRelations*, unsigned, unsigned, unsigned);
  /// constructor
  Joiner(uint32_t);
  ~Joiner();
};
//---------------------------------------------------------------------------
class JoinerArgs{
public:
  Joiner* obj;
  Query* query;
  char** output;
  Relation** rels;
  int relSize;
  int id;

  JoinerArgs(Joiner* o, Query* q, char** out, int i, Relation** r, int s)
   :obj(o), query(q), output(out), id(i), rels(r), relSize(s){}
};
