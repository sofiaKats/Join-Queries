#include <fstream>
#include <string>
#include <sstream>
#include "../partition-phase/Structures.h"
#include <dirent.h>

class Parser{
private:
  Column* ParseRelation(string);

public:
  Column** ReadRelations(char*, int&);
  int** ReadQueries(int&);
};
