#pragma once
#include "../Parsing/Operators.hpp"
#include "../../partition-phase/PartitionedHashJoin.h"
#include "../../partition-phase/Structures.h"

//---------------------------------------------------------------------------
class Joiner {

  public:
  /// The relations that might be joined
  std::vector<Relation> relations;
  /// Add relation
  void AddRelation(const char* fileName);
  /// Get relation
  Relation& GetRelation(unsigned id);
  /// Get relation column
  RelColumn* GetRelationCol(unsigned, unsigned);
  /// Joins a given set of relations
  string Join(QueryInfo& i);
};
//---------------------------------------------------------------------------
