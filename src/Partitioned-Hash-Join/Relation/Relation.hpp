#pragma once
#include <cstdint>
#include <string>
#include <vector>

using RelationId = unsigned;
//---------------------------------------------------------------------------
class Relation {
  private:
  /// Owns memory (false if it was mmaped)
  bool ownsMemory;
  /// Loads data from a file
  void loadRelation(const char* fileName);

  public:
  /// The number of tuples
  uint64_t size;
  /// The join column containing the keys
  std::vector<uint64_t*> columns;

  /// Constructor without mmap
  Relation(uint64_t size,std::vector<uint64_t*>&& columns) : ownsMemory(true), size(size), columns(columns) {}
  /// Constructor using mmap
  Relation(const char* fileName);
  /// Delete copy constructor
  Relation(const Relation& other)=delete;
  /// Move constructor
  Relation(Relation&& other)=default;
  /// The destructor
  ~Relation();
};
