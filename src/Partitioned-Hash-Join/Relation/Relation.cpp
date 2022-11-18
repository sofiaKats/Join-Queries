#include <fcntl.h>
#include <iostream>
#include <fstream>
#include <sys/mman.h>
#include <sys/stat.h>
#include "Relation.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------

void Relation::loadRelation(const char* fileName)
{
  int fd = open(fileName, O_RDONLY);
  if (fd==-1) {
    throw runtime_error("* cannot open file");
  }
  // Obtain file size
  struct stat sb;
  if (fstat(fd,&sb)==-1){
    throw runtime_error("* fstat");
  }

  auto length=sb.st_size;

  if (length<16) {
    throw runtime_error("* file does not contain a valid header");
  }
  char* addr=static_cast<char*>(mmap(nullptr,length,PROT_READ,MAP_PRIVATE,fd,0u));
  if (addr==MAP_FAILED) {
    throw runtime_error("* cannot mmap file");
  }

  this->size=*reinterpret_cast<uint64_t*>(addr);
  addr+=sizeof(size);
  this->numColumns=*reinterpret_cast<size_t*>(addr);
  //cout << "Num of columns is " << numColumns << endl;
  addr+=sizeof(size_t);

  columnsArr = new uint64_t*[numColumns];

  for (unsigned i=0;i<numColumns;++i) {
    this->columns.push_back(reinterpret_cast<uint64_t*>(addr));
    //create array
    columnsArr[i] = new uint64_t[size];
    columnsArr[i] = reinterpret_cast<uint64_t*>(addr);

    addr+=size*sizeof(uint64_t);
  }
  cout << "- done!\n";
}
//---------------------------------------------------------------------------
Relation::Relation(const char* fileName) : ownsMemory(false)
  // Constructor that loads relation from disk
{
  cout << "- loading relation..." << endl;
  loadRelation(fileName);
}
//---------------------------------------------------------------------------
Relation::~Relation()
  // Destructor
{
  if (ownsMemory) {
    for (auto c : columns)
      delete[] c;
  }
}
