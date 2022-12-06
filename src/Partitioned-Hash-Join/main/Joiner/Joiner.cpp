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
  int notNUllRow = getFirstURrow();
  if (notNUllRow == -1) return GetRelationCol(relationId, colId);
  //if (usedRelations->matchRows[0] == NULL) return GetRelationCol(relationId, colId);
  if (usedRelations->matchRows[notNUllRow]->arr[relationId] == -1){
    //relation does not exist in usedRelations
    return GetRelationCol(relationId, colId);
  }
  cout << "First not null Row is " << notNUllRow << endl;
  Relation& rel = GetRelation(relationId);
  RelColumn* relColumn = new RelColumn(relationId, usedRelations->activeSize);
  //changed sth here!!!!!!!!!!!!!!!!!!!!!!!
  int relCol_cnt = 0;
  for (int i = 0; i < usedRelations->size; i++){
    if (usedRelations->matchRows[i] == NULL) continue;    
    
    relColumn->tuples[relCol_cnt].key = usedRelations->matchRows[i]->arr[relationId];
    relColumn->tuples[relCol_cnt].payload = rel.columns[colId][usedRelations->matchRows[i]->arr[relationId]];
    relCol_cnt++;
  }
  return relColumn;
}
//-----------------------------------------------------------------------
void Joiner::UpdateUsedRelations(Matches* matches, int relRid, int relSid){

  if (matches == NULL){
    // TODO implement (clears out usedRelations)
    return;
  }
  if (firstJoin){ /// First Join
    firstJoin = false;
    cout << "[UpdateUR] First Join" << endl;
    updateURFirst(matches, relRid, relSid);
    //cout << "[UpdateUR] First Join done!" << endl;
    return;
  }

  int relRVal;
  int relSVal;
  for (uint32_t i=0; i<usedRelations->size; i++){ /// Find first not null entry in usedRelations
    if (usedRelations->matchRows[i] != NULL){
      relRVal = usedRelations->matchRows[i]->arr[relRid];
      relSVal = usedRelations->matchRows[i]->arr[relSid];
    } break;
  }

  // CASE 2.1: Only one of the Relations has been joined before, the Relation R.
  // Or both
  if (relRVal != -1){
    cout << "[UpdateUR] Left or Both" << endl;
    updateURonlyR(matches, relRid, relSid);
    //cout << "[UpdateUR] Left or Both done!" << endl;
    return;
  }
  // CASE 2.2: Only one of the Relations has been joined before, the Relation S.
  if (relSVal != -1){
    cout << "[UpdateUR] Right" << endl;
    updateURonlyS(matches, relSid, relRid);
    return;
  }

}

void Joiner::updateURFirst(Matches* matches, int relRid, int relSid){
  uint32_t i;
  for (i=0; i<matches->activeSize; i++){
    usedRelations->matchRows[i] = new MatchRow(usedRelations->rowSize);
    usedRelations->matchRows[i]->arr[relRid] = matches->tuples[i]->key;
    usedRelations->matchRows[i]->arr[relSid] = matches->tuples[i]->payload;
  }
  usedRelations->activeSize = i;
}

void Joiner::updateURonlyR(Matches* matches, int relUR, int relNew){
  int deleteions = 0;
  for (uint32_t i=0; i<usedRelations->size; i++){ /// For each entry from usedRelations
    if (usedRelations->matchRows[i] == NULL) continue;

    bool del = true;
    uint32_t rowid = usedRelations->matchRows[i]->arr[relUR];
    for (uint32_t j=0; j<matches->activeSize; j++){ /// Check if exists in new match table
      if (rowid == matches->tuples[j]->key){
        //cout << rowid<<":"<<matches->tuples[j]->payload<<endl;
        usedRelations->matchRows[i]->arr[relNew] = matches->tuples[j]->payload;
        del = false;
        break;
      }
    }
    if (del){
      deleteions ++;
      delete usedRelations->matchRows[i];
      usedRelations->matchRows[i] = NULL;
      usedRelations->activeSize--;
    }
  }
}

void Joiner::updateURonlyS(Matches* matches, int relUR, int relNew){
  for (uint32_t i=0; i < usedRelations->size; i++){ /// For each entry from usedRelations
    if (usedRelations->matchRows[i] == NULL) continue;

    bool del = true;
    uint32_t rowid = usedRelations->matchRows[i]->arr[relUR];
    for (uint32_t j=0; j<matches->activeSize; j++){ /// Check if exists in new match table
      if (rowid == matches->tuples[j]->payload){
        usedRelations->matchRows[i]->arr[relNew] = matches->tuples[j]->key;
        del = false;
        break;
      }
    }
    if (del){
      delete usedRelations->matchRows[i];
      usedRelations->matchRows[i] = NULL;
      usedRelations->activeSize--;
    }
  }
}

//-----------------------------------------------------------------------
string Joiner::Join(Query& query)
  // Executes a join query
{
  usedRelations = new UsedRelations(2000, query.number_of_relations);

  ///First Join

  //TEMP OF 3 JOIN, this later will be query.prdcts->activeSize
  for (int i = 0; i < 3; i++){
    RelColumn* relR = GetUsedRelation((unsigned)query.prdcts[i]->relation_index_left, (unsigned)query.prdcts[i]->column_left);
    RelColumn* relS = GetUsedRelation((unsigned)query.prdcts[i]->relation_index_right, (unsigned)query.prdcts[i]->column_right);
    if (isSelfJoin((unsigned)query.prdcts[i]->relation_index_left, (unsigned)query.prdcts[i]->relation_index_right)){
      updateURselfJoin((unsigned)query.prdcts[i]->relation_index_left, selfJoin(relR, relS));
    }
    else {
      PartitionedHashJoin* phj = new PartitionedHashJoin(relR, relS);
      UpdateUsedRelations(phj->Solve(),(unsigned)query.prdcts[i]->relation_index_left, (unsigned)query.prdcts[i]->relation_index_right);
      delete phj;
    }
    delete relR;
    delete relS;    
  }


  printUsedRelations();


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

  //delete usedRelations;

  //return "- result -\n";
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
  cout << "\n--- Join Results in UR table---\n\n" << endl;
  for (int i=0; i<usedRelations->size; i++){
    if (usedRelations->matchRows[i] != nullptr) {
      for (int j = 0; j < usedRelations->matchRows[i]->size; j++) {
        cout << usedRelations->matchRows[i]->arr[j] << " ";
      }
      cout << endl;
    }
    // if (i == 10) {
    //   break;
    // }
  }
}

bool Joiner::isSelfJoin(unsigned int relR, unsigned int relS){
  return relR == relS;
}

void Joiner::updateURselfJoin(int relId, SingleCol* sc){
  cout << "[UpdateUR] Self Join" << endl;
  if (firstJoin){
    firstJoin = false;
    for (int i = 0; i < sc->activeSize; i++){
      usedRelations->matchRows[i] = new MatchRow(usedRelations->rowSize);
      usedRelations->matchRows[i]->arr[relId] = sc->arr[i];
    }
    return;
  }

  for (int i = 0; i < usedRelations->size; i++){
    if (usedRelations->matchRows[i] == NULL) continue;

    bool del = true;
    int rowid = usedRelations->matchRows[i]->arr[relId];
    for (uint32_t j=0; j < sc->activeSize; j++){ /// Check if exists in new match table
      //
      if (rowid == sc->arr[j]){
        del = false;
        break;
      }
    }
    if (del){
      delete usedRelations->matchRows[i];
      usedRelations->matchRows[i] = NULL;
      usedRelations->activeSize--;
    }    
  } 
  cout << "[UpdateUR] Self Join done!" << endl; 
}

SingleCol* Joiner::selfJoin(RelColumn* relR, RelColumn* relS){
  SingleCol* singleCol = new SingleCol(relR->num_tuples);
  for (int i = 0; i < relR->num_tuples; i++){
    if (i >= relS->num_tuples){
      return singleCol;
    }
    if (relR->tuples[i].payload == relS->tuples[i].payload){
      singleCol->arr[singleCol->activeSize] = relR->tuples[i].key;
      // cout << "relR->tuples[i].key: " << relR->tuples[i].key << " == " << relS->tuples[i].key << endl;
      singleCol->activeSize++;
    }
  }
  return singleCol;
}

int Joiner::getFirstURrow(){
  int i;
  if (firstJoin) return -1;
  for (i = 0; i < usedRelations->size; i++){
    if (usedRelations->matchRows[i] != NULL){
      return i;
    }
  }
  if (i == usedRelations->size) return -1;
}