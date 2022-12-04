#include "Joiner.hpp"

Joiner::Joiner(uint32_t size, uint32_t rowSize){
  this->size = size;
  this->rowSize = rowSize;
  //->usedRelations = rowSize;
  relations = new Relation*[size]{NULL};
  for (int i =0; i < size; i++){
    relations[i] = NULL;
  }
}

void Joiner::AddRelation(const char* fileName)
// Loads a relation from disk
{
  for (int i = 0; i < size; i++)
  if (relations[i] == NULL){
    relations[i] = new Relation(fileName, i);
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
void Joiner::UpdateUsedRelations(Matches* matches, int relRid, int relSid){

  if (matches == NULL){
    // TODO implement (clears out usedRelations)
    return;
  }

  if (firstJoin){                                   /// First Join
    cout << "FIRST!" << endl;
    firstJoin = false;
    updateURFirst(matches, relRid, relSid);
    return;
  }
  cout << "NOT HERE!";
  
  for (uint32_t i=0; i < usedRelations->size; i++){ /// Find First Not null entry in usedRelations
    if (usedRelations->matchRows[i] == NULL) continue;

    //CASE 1: This can't happen techincally.
    if ((usedRelations->matchRows[i]->arr[relRid] == -1) && (usedRelations->matchRows[i]->arr[relSid] == -1)){
      //updateURFirst(matches, relRid, relSid);
      return;
    }

    //CASE 2.1: Only one of the Relations has been joined before, the Relation R.
    if ((usedRelations->matchRows[i]->arr[relRid] != -1) && (usedRelations->matchRows[i]->arr[relSid] == -1)){
      updateURonlyR(matches, relRid, relSid);
      return;
    }

    //CASE 2.2: Only one of the Relations has been joined before, the Relation S.
    if ((usedRelations->matchRows[i]->arr[relRid] == -1) && (usedRelations->matchRows[i]->arr[relSid] != -1)){
      updateURonlyS(matches, relSid, relRid);
      return;
    }

    //CASE 3: Both of the Relations have been joined before.
    if ((usedRelations->matchRows[i]->arr[relRid] != -1) && (usedRelations->matchRows[i]->arr[relSid] != -1)){
      return;
    }
  }
}
/*
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
*/

void Joiner::updateURFirst(Matches* matches, int relRid, int relSid){
  uint32_t c = 0;
  
  for (uint32_t i=0; i<matches->activeSize; i++){
    usedRelations->matchRows[c] = new MatchRow(usedRelations->rowSize);
    usedRelations->matchRows[c]->arr[relRid] = matches->tuples[i]->key;
    usedRelations->matchRows[c++]->arr[relSid] = matches->tuples[i]->payload;
    usedRelations->activeSize++;
  }
}

void Joiner::updateURonlyR(Matches* matches, int relUR, int relNew){
  for (uint32_t i=0; i < usedRelations->size; i++){ /// For each entry from usedRelations
    if (usedRelations->matchRows[i] == NULL) continue;

    bool del = true;
    uint32_t rowid = usedRelations->matchRows[i]->arr[relUR];
    for (uint32_t i=0; i<matches->activeSize; i++){ /// Check if exists in new match table
      if (rowid == matches->tuples[i]->key){
        del = false;
        break;
      }
    }
    if (del){
      delete usedRelations->matchRows[i];
      usedRelations->matchRows[i] = NULL;
      usedRelations->activeSize--;
    }
    else {
      usedRelations->matchRows[i]->arr[relNew] = matches->tuples[i]->payload;
    }
  }
}

void Joiner::updateURonlyS(Matches* matches, int relUR, int relNew){
  for (uint32_t i=0; i < usedRelations->size; i++){ /// For each entry from usedRelations
    if (usedRelations->matchRows[i] == NULL) continue;

    bool del = true;
    uint32_t rowid = usedRelations->matchRows[i]->arr[relUR];
    for (uint32_t i=0; i<matches->activeSize; i++){ /// Check if exists in new match table
      if (rowid == matches->tuples[i]->payload){
        del = false;
        break;
      }
    }
    if (del){
      delete usedRelations->matchRows[i];
      usedRelations->matchRows[i] = NULL;
      usedRelations->activeSize--;
    }
    else {
      usedRelations->matchRows[i]->arr[relNew] = matches->tuples[i]->key;
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
  UpdateUsedRelations(phj->Solve(),(unsigned)query.prdcts[0]->relation_index_left, (unsigned)query.prdcts[0]->relation_index_right);
  printUsedRelations();

  delete phj;
  delete relR;
  delete relS;

  // ///Second Join
  // relR = GetUsedRelation((unsigned)query.prdcts[1]->relation_index_left, (unsigned)query.prdcts[1]->column_left);
  // relS = GetUsedRelation((unsigned)query.prdcts[1]->relation_index_right, (unsigned)query.prdcts[1]->column_right);
  // phj = new PartitionedHashJoin(relR, relS);
  // UpdateUsedRelations(phj->Solve(), (unsigned)query.prdcts[1]->relation_index_left, (unsigned)query.prdcts[1]->relation_index_right);

  // delete phj;
  // delete relR;
  // delete relS;

  //for (int i=0; i<relR->num_tuples; i++)
    //cout << relR->tuples[i].key<< endl;

  // delete relR;

  // /*for (unsigned i=1; i<query.prdcts.size(); ++i){
  //   relR = GetUsedRelation((unsigned)query.prdcts[i]->relation_index_left, (unsigned)query.prdcts[i]->column_left);
  //   relS = GetUsedRelation((unsigned)query.prdcts[i]->relation_index_right, (unsigned)query.prdcts[i]->column_right);
  //   PartitionedHashJoin* phj = new PartitionedHashJoin(relR, relS);
  //   phj->Solve(*usedRelations);
  // }*/

  // return to_string(Checksum(0,1));

  // /*
  // stringstream out;
  // auto& results=checkSum.checkSums;
  // for (unsigned i=0;i<results.size();++i) {
  //   out << (checkSum.resultSize==0?"NULL":to_string(results[i]));
  //   if (i<results.size()-1)
  //     out << " ";
  // }
  // out << "\n";
  // return out.str();
  // */

  // delete usedRelations;

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

void Joiner::printUsedRelations(){
  for (int i=0; i<usedRelations->activeSize; i++){
    if (usedRelations->matchRows[i] != nullptr) {
      for (int j = 0; j < usedRelations->matchRows[i]->size; j++) {
        cout << usedRelations->matchRows[i]->arr[j] << " ";
      }
      cout << endl;
    }
    if (i == 10) {
      break;
    }
  }
}
