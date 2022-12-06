#pragma once
#include "../../partition-phase/PartitionedHashJoin.h"
#include "../../partition-phase/Structures.h"
#include "../../Parser/src/parser.h"

//---------------------------------------------------------------------------
class Joiner {
private:
  bool firstJoin = true;
  void updateURFirst(Matches*, int, int);
  void updateURonlyR(Matches*, int, int);
  void updateURonlyS(Matches*, int, int);
  void updateURselfJoin(int, SingleCol*);


  bool isSelfJoin(unsigned int, unsigned int);
  SingleCol* selfJoin(RelColumn*, RelColumn*);

public:
  /// The relations that might be joined
  Relation** relations;
  UsedRelations* usedRelations;
  uint32_t size;
  uint32_t rowSize;
  /// Add relation
  void AddRelation(const char* fileName);
  /// Get relation
  Relation& GetRelation(unsigned id);
  /// Get relation column
  RelColumn* GetRelationCol(unsigned, unsigned);
  /// Get relation column from filtered relation
  RelColumn* GetUsedRelation(unsigned, unsigned);
  /// Joins a given set of relations
  string Join(Query&);

  void UpdateUsedRelations(Matches*, int, int);
  
  void printUsedRelations();

  /// Checksum
  uint64_t Checksum(unsigned, unsigned);
  /// constructor
  Joiner(uint32_t, uint32_t);
  ~Joiner();
};
//---------------------------------------------------------------------------
