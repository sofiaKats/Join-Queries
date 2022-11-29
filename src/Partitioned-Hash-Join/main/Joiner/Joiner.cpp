#include "Joiner.hpp"

Joiner::Joiner(uint32_t size, uint32_t rowSize){
  this->size = size;
  this->rowSize = rowSize;
  //->usedRelations = rowSize;
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
    RelColumn* relColumn = new RelColumn(relationId, rel.size);
    for (int i = 0; i < rel.size; i++){
      relColumn->tuples[i].payload = rel.columns[colId][i];
      //cout << relColumn->tuples[i].key << " " << relColumn->tuples[i].payload << endl;
    }
    return relColumn;
}

RelColumn* Joiner::GetUsedRelation(unsigned relationId, unsigned colId){
  Relation& rel = GetRelation(relationId);
  RelColumn* relColumn = new RelColumn(relationId, usedRelations->size);
  for (int i = 0; i < rel.size; i++){
    relColumn->tuples[i].payload = rel.columns[colId][i];
    //cout << relColumn->tuples[i].key << " " << relColumn->tuples[i].payload << endl;
  }
  return relColumn;
}
//-----------------------------------------------------------------------
string Joiner::Join(Query& query)
  // Executes a join query
{
  UsedRelations* usedRelations = new UsedRelations(2000, query.number_of_relations);

  RelColumn* relR = GetRelationCol((unsigned)query.prdcts[0]->relation_index_left,(unsigned) query.prdcts[0]->column_left);
  RelColumn* relS = GetRelationCol((unsigned)query.prdcts[0]->relation_index_right,(unsigned) query.prdcts[0]->column_right);

  PartitionedHashJoin* phj = new PartitionedHashJoin(relR, relS);
  phj->Solve(*usedRelations);

  for (int i=0; i<usedRelations->activeSize; i++)
    cout << "Matched rows " << usedRelations->matchRows[i]->arr[0] << " " << usedRelations->matchRows[i]->arr[1] << endl;

  // for (unsigned i=1; i<query.predicates.size(); ++i){
  //   //GetUsedRelation();
  // }

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

  delete usedRelations;
  delete relR;
  delete relS;
  delete phj;
  return "- result -\n";
}

Joiner::~Joiner(){
  for (int i = 0; i<size; i++){
    delete relations[i];
  }
  delete[] relations;
}
