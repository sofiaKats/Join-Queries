#pragma once
#include "../Parsing/Operators.hpp"
#include "../../partition-phase/PartitionedHashJoin.h"
#include "../../partition-phase/Structures.h"

//---------------------------------------------------------------------------
class Joiner {

  public:
  /// The relations that might be joined
  Relation** relations;
  UsedRelations* usedRelations;
  uint32_t size;
  uint32_t rowSize;
  /// Add relation
  void AddRelation(const char* fileName);
  /// Get relation
  Relation& GetRelation(uint32_t id);
  /// Get relation column
  RelColumn* GetRelationCol(uint32_t, uint32_t);
  /// Get relation column from filtered relation
  RelColumn* GetUsedRelation(uint32_t, uint32_t);
  /// Joins a given set of relations
  string Join(QueryInfo& i);
  /// constructor
  Joiner(uint32_t, uint32_t);
  ~Joiner();
};
//---------------------------------------------------------------------------
