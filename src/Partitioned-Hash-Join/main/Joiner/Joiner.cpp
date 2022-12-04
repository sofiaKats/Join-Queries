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
      relColumn->tuples[i].key = i;
      relColumn->tuples[i].payload = rel.columns[colId][i];
    }
    return relColumn;
}

RelColumn* Joiner::GetUsedRelation(unsigned relationId, unsigned colId){
  if (usedRelations->matchRows[0] == NULL || usedRelations->matchRows[0]->arr[relationId] == -1){
    //relation does not exist in usedRelations
    return GetRelationCol(relationId, colId);
  }
  Relation& rel = GetRelation(relationId);
  RelColumn* relColumn = new RelColumn(relationId, usedRelations->activeSize);
  for (int i = 0; i < usedRelations->activeSize; i++){
    relColumn->tuples[i].key = usedRelations->matchRows[i]->arr[relationId];
    relColumn->tuples[i].payload = rel.columns[colId][usedRelations->matchRows[i]->arr[relationId]];
  }
  return relColumn;
}
//-----------------------------------------------------------------------
void Joiner::UpdateUsedRelations(UsedRelations& usedRelations, Matches* matches){

  if (matches == NULL){
    // TODO implement (clears out usedRelations)
    return;
  }
  if (firstJoin){ /// First Join
    uint32_t c = 0;
    firstJoin = false;
    cout << "first\n";
    //rel->id must be the binding
    for (uint32_t i=0; i<matches->activeSize; i++){
      usedRelations.matchRows[c] = new MatchRow(usedRelations.rowSize);
      usedRelations.matchRows[c]->arr[p1->rel->id] = matches->tuples[i]->key;
      usedRelations.matchRows[c++]->arr[p2->rel->id] = matches->tuples[i]->payload;
    }
  }else{
    cout << "left\n";
    if (usedRelations.matchRows[0]->arr[p1->rel->id] != -1){
      for (uint32_t i=0; i<usedRelations.size; i++){ /// For each entry from usedRelations
        if (usedRelations.matchRows[i] == NULL) continue;
        bool del = true;
        uint32_t rowid = usedRelations.matchRows[i]->arr[p1->rel->id];
        for (uint32_t i=0; i<matches->activeSize; i++){ /// Check if exists in new match table
          if (rowid == matches->tuples[i]->key){
            del = false;
            break;
          }
        }
        if (del){
          delete usedRelations.matchRows[i];
          usedRelations.matchRows[i] = NULL;
          usedRelations.activeSize--;
        }
      }
    }
    else if (usedRelations.matchRows[0]->arr[p2->rel->id] != -1){
      cout << "right\n";

      for (uint32_t i=0; i<usedRelations.size; i++){ /// For each entry from usedRelations
        if (usedRelations.matchRows[i] == NULL) continue;
        bool del = true;
        uint32_t rowid = usedRelations.matchRows[i]->arr[p2->rel->id];
        for (uint32_t i=0; i<matches->activeSize; i++){ /// Check if exists in new match table
          if (rowid == matches->tuples[i]->payload){
            del = false;
            break;
          }
        }
        if (del){
          delete usedRelations.matchRows[i];
          usedRelations.matchRows[i] = NULL;
          usedRelations.activeSize--;
        }
      }
    }
  }
}
//-----------------------------------------------------------------------
string Joiner::Join(Query& query)
  // Executes a join query
{
  usedRelations = new UsedRelations(2000, query.number_of_relations);

  ///First Join
  RelColumn* relR = GetRelationCol((unsigned)query.prdcts[0]->relation_index_left,(unsigned)query.prdcts[0]->column_left);
  RelColumn* relS = GetRelationCol((unsigned)query.prdcts[0]->relation_index_right,(unsigned)query.prdcts[0]->column_right);
  PartitionedHashJoin* phj = new PartitionedHashJoin(relR, relS);
  UpdateUsedRelations(*usedRelations, phj->Solve());


  delete phj;
  delete relR;
  delete relS;

  //for (int i=0; i<20; i++)
    //cout << "Matched rows " << usedRelations->matchRows[i]->arr[0] << " " << usedRelations->matchRows[i]->arr[1] << endl;

  ///Other Joins
  relR = GetUsedRelation((unsigned)query.prdcts[1]->relation_index_left, (unsigned)query.prdcts[1]->column_left);
  relS = GetUsedRelation((unsigned)query.prdcts[1]->relation_index_right, (unsigned)query.prdcts[1]->column_right);
  phj = new PartitionedHashJoin(relR, relS);
  phj->Solve(*usedRelations);

  //for (int i=0; i<relR->num_tuples; i++)
    //cout << relR->tuples[i].key<< endl;

  delete relR;

  /*for (unsigned i=1; i<query.prdcts.size(); ++i){
    relR = GetUsedRelation((unsigned)query.prdcts[i]->relation_index_left, (unsigned)query.prdcts[i]->column_left);
    relS = GetUsedRelation((unsigned)query.prdcts[i]->relation_index_right, (unsigned)query.prdcts[i]->column_right);
    PartitionedHashJoin* phj = new PartitionedHashJoin(relR, relS);
    phj->Solve(*usedRelations);
  }*/

  return to_string(Checksum(0,1));

  /*
  stringstream out;
  auto& results=checkSum.checkSums;
  for (unsigned i=0;i<results.size();++i) {
    out << (checkSum.resultSize==0?"NULL":to_string(results[i]));
    if (i<results.size()-1)
      out << " ";
  }
  out << "\n";
  return out.str();
  */

  delete usedRelations;

  return "- result -\n";
}

uint64_t Joiner::Checksum(unsigned relationId, unsigned colId){
  uint64_t sum = 0;
  Relation& rel = GetRelation(relationId);
  for (int i = 0; i < usedRelations->size; i++){
    if (usedRelations->matchRows[i] == NULL) continue;
    sum += rel.columns[colId][usedRelations->matchRows[i]->arr[relationId]];
  }
  return sum;
}

Joiner::~Joiner(){
  for (int i = 0; i<size; i++){
    delete relations[i];
  }
  delete[] relations;
}
