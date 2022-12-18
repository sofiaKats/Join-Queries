#include <fcntl.h>
#include <iostream>
#include <fstream>
#include <sys/mman.h>
#include <sys/stat.h>
#include "Relation.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
//-                        Relation Functions                               -
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
  addr+=sizeof(size_t);

  columns = new uint64_t*[numColumns];

  for (unsigned i=0;i<numColumns;++i) {
    //create array
    columns[i] = new uint64_t[size];
    columns[i] = reinterpret_cast<uint64_t*>(addr);

    addr+=size*sizeof(uint64_t);
  }
  close(fd);
}

//---------------------------------------------------------------------------
Relation::Relation(const char* fileName, int id) : ownsMemory(false)
  // Constructor that loads relation from disk
{
  loadRelation(fileName);
  this->id = id;
  column_metadata = new Metadata*[numColumns];
  for(int colId=0; colId<numColumns; colId++) column_metadata[colId] = new Metadata(this->size);
  findMINValueOfColumns();
  findMAXValueOfColumns();
  // for each column struct, allocate memory for distinct array
  // and fill it with values so that we find the value of d
  for(int colId=0; colId<numColumns; colId++) column_metadata[colId]->InitDistinctArr();
  findDistinctValues();
}
//---------------------------------------------------------------------------
Relation::~Relation()
  // Destructor
{
  for(int colId=0; colId<numColumns; colId++){ 
    delete column_metadata[colId];
    // delete [] columns[colId];
  }
  delete [] column_metadata;
  delete [] columns;
}

int Relation::getId(){return id;}

//---------------------------------------------------------------------------
void Relation::findMINValueOfColumns(void){
  // for each column there is
  for(int colId=0; colId<numColumns; colId++) {
    // find the minimum value of the column,and set l value to min
    unsigned long int min = columns[colId][0];

    for(int rowId=1; rowId<size; rowId++) 
      if(columns[colId][rowId] < min) min = columns[colId][rowId];
    
    column_metadata[colId]->setL(min);
    //cout << "min value of cold: " << colId << " is " << min << endl;
  }
}

//---------------------------------------------------------------------------
void Relation::findMAXValueOfColumns(void){
  // for each column there is
  for(int colId=0; colId<numColumns; colId++) {
    // find the maximum value of the column,and set u value to max
    unsigned long int max = columns[colId][0];

    for(int rowId=1; rowId<size; rowId++) 
      if(columns[colId][rowId] > max) max = columns[colId][rowId];
    
    column_metadata[colId]->setU(max);
    //cout << "max value of cold: " << colId << " is " << max << endl;
  }
}

//---------------------------------------------------------------------------
void Relation::findDistinctValues(void) {
  unsigned long int position;
  for(int colId=0; colId<numColumns; colId++) {
    for(int rowId=1; rowId<size; rowId++) {

      if(column_metadata[colId]->getSizeOfDistinctArray() < NVALUE)
      // if array size < N, we change index ~ x-u ~
        position = (columns[colId][rowId] > column_metadata[colId]->getU()) ? columns[colId][rowId] - column_metadata[colId]->getU() : column_metadata[colId]->getU() - columns[colId][rowId];
      else 
        // if array size >= N, the we change index ~ x-l mod N ~
        position = (columns[colId][rowId] > column_metadata[colId]->getL()) ? (columns[colId][rowId] - column_metadata[colId]->getL()) % NVALUE : (column_metadata[colId]->getL() - columns[colId][rowId]) % NVALUE;

      //we are looking for distinct values only, if the index is false we increase d by 1
      if(column_metadata[colId]->distinct_arr[position] != true) {
        column_metadata[colId]->distinct_arr[position] = true;
        column_metadata[colId]->increase_d_value_by_1();
      }
    }
    //cout << "d value of colId: " << colId << " is " << column_metadata[colId]->getD() << endl;
  }
}

//---------------------------------------------------------------------------
//-                        Metadata Functions                               -
//---------------------------------------------------------------------------

Metadata::Metadata(uint64_t size):l(0), u(0), f(size), d(-1) { /*cout << "f is: " << f << endl;*/ }

Metadata::~Metadata() { delete [] distinct_arr; }

// returns mininmum value of a particular column
uint64_t Metadata::getL(void){ return l; }

// sets mininmum value of a particular column
void Metadata::setL(uint64_t L){ l=L; }

// returns maximum value of a particular column
uint64_t Metadata::getU(void){ return u; }

// sets maxium value of a particular column
void Metadata::setU(uint64_t U){ u=U; }

void Metadata::InitDistinctArr(void) {
  size = u-l+1;
  if(size > NVALUE) size = NVALUE;
  distinct_arr = new bool[size];
  // initialize array with zeros
  for(int i=0; i<size; i++) distinct_arr[i] = false;
}

int Metadata::getSizeOfDistinctArray(void){ return size; }

// set the amount of distincted values of column
void Metadata::setD(int D){ d=D; }

// get the amount of distincted values of column
int Metadata::getD(void){ return d; }

void Metadata::increase_d_value_by_1(void){ d+=1; }

void Metadata::decrease_d_value_by_1(void){ d-=1; }