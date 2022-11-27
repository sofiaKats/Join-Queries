#include "Joiner.hpp"

Joiner::Joiner(int size){
  this->size = size;
  relations = new Relation*[size]{NULL};
}

void Joiner::AddRelation(const char* fileName)
// Loads a relation from disk
{
  for (int i = 0; i < size; i++)
  if (relations[i] == NULL){
    relations[i] = new Relation(fileName);
    return;
  }
}
//---------------------------------------------------------------------------
Relation& Joiner::GetRelation(unsigned relationId)
// Loads a relation from disk
{
  if (relationId >= size) {
    char error[256];
    sprintf(error, "* relation with id: %d does not exist", relationId);
    throw runtime_error(error);
  }
  return *relations[relationId];
}

RelColumn* Joiner::GetRelationCol(unsigned relationId, unsigned colId){
    Relation& rel = GetRelation(relationId);
    RelColumn* relColumn = new RelColumn(rel.size);
    for (int i = 0; i < rel.size; i++){
      relColumn->tuples[i].key = i;
      relColumn->tuples[i].payload = rel.columns[colId][i];
      cout << relColumn->tuples[i].key << " " << relColumn->tuples[i].payload << endl;
    }
    return relColumn;
}
//-----------------------------------------------------------------------
string Joiner::Join(QueryInfo& query)
  // Executes a join query
{ //"3 0 1|0.2=1.0&0.1=2.0&0.2>3000|1.2 0.1";
  GetRelationCol(query.predicates[0].left.relId, query.predicates[0].left.colId);



  /*Checksum checkSum(move(root),query.selections);
  checkSum.run();

  stringstream out;
  auto& results=checkSum.checkSums;
  for (unsigned i=0;i<results.size();++i) {
    out << (checkSum.resultSize==0?"NULL":to_string(results[i]));
    if (i<results.size()-1)
      out << " ";
  }
  out << "\n";
  return out.str();*/
}
