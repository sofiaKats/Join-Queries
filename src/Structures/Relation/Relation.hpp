#pragma once
#include <cstdint>
#include <unistd.h>
#include <cstdlib>

#define NVALUE 1000000 // any number less than  50.000.000

//---------------------------------------------------------------------------
// this is where we store {lΑ,uA,fA,dA} values
class Metadata {
private:
  unsigned long int l;  // minimum value in column
  unsigned long int u;  // maximum value in column
  int f;                // number of values in column
  int d;                // number of distinct values in column
  int size;             // size of distinct_arr
public:
  bool* distinct_arr;    // array used to find d value faster
  Metadata(uint64_t size);
  ~Metadata();
  uint64_t getL(void);
  void setL(uint64_t L);
  uint64_t getU(void);
  void setU(uint64_t U);
  void InitDistinctArr(void);
  int getSizeOfDistinctArray(void);
  void setD(int D);
  int getD(void);
  void increase_d_value_by_1(void);
  void decrease_d_value_by_1(void);
};

using RelationId = unsigned;
//---------------------------------------------------------------------------
class Relation {
  private:
  /// Owns memory (false if it was mmaped)
  bool ownsMemory;
  /// mmap address
  char* address;
  /// mmap length
  size_t length;
  /// Loads data from a file
  void loadRelation(const char* fileName);

  public:
  /// The number of rows
  uint64_t size;
  /// The number of tuples/columns
  uint64_t numColumns;
  /// The join column containing the keys
  uint64_t** columns;
  Metadata** column_metadata; // each column has a Metadata struct of it's own.
  /// Constructor using mmap
  Relation(const char* fileName);
  /// Delete copy constructor
  Relation(const Relation& other)=delete;
  /// Move constructor
  Relation(Relation&& other)=default;
  /// The destructor
  ~Relation();

  void findMINValueOfColumns(void);
  void findMAXValueOfColumns(void);
  void findDistinctValues(void);
};
