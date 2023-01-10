#include "Joiner.hpp"

Joiner::Joiner(uint32_t size){
  relations = new Relation*[size]{};
}
//-----------------------------------------------------------------------
void Joiner::AddRelation(const char* fileName){
    relations[numRelations++] = new Relation(fileName);
}
//---------------------------------------------------------------------------
Relation& Joiner::GetRelation(unsigned relationId){
  if (relationId >= numRelations) {
    char error[256];
    sprintf(error, "* relation with id: %d does not exist", relationId);
    throw runtime_error(error);
  }
  return *relations[relationId];
}
//-----------------------------------------------------------------------
RelColumn* Joiner::GetRelationCol(unsigned relationId, unsigned colId){
    Relation& rel = GetRelation(relationId);
    RelColumn* relColumn = new RelColumn(rel.size);
    for (int i = 0; i < rel.size; i++){
      relColumn->tuples[i].key = i;
      relColumn->tuples[i].payload = rel.columns[colId][i];
    }
    return relColumn;
}
//-----------------------------------------------------------------------
RelColumn* Joiner::GetUsedRelation(UsedRelations* usedRelations, unsigned relationId, unsigned binding, unsigned colId){
  int firstRow = getFirstURrow(usedRelations);
  if (firstRow == -1 || usedRelations->matchRows[firstRow]->arr[binding] == -1)
    return GetRelationCol(relationId, colId);

  Relation& rel = GetRelation(relationId);
  RelColumn* relColumn = new RelColumn(usedRelations->activeSize);

  relColumn->num_tuples = 0;
  for (int i=0; i<usedRelations->size; i++){
    if (usedRelations->matchRows[i] == NULL) continue;

    uint32_t rowid = usedRelations->matchRows[i]->arr[binding];
    relColumn->tuples[relColumn->num_tuples].key = rowid;
    relColumn->tuples[relColumn->num_tuples++].payload = rel.columns[colId][rowid];
  }
  return relColumn;
}
//-----------------------------------------------------------------------
void* Joiner::thread_executeQuery(void* vargp){
  JoinerArgs* args = (JoinerArgs*)vargp;
  Joiner* obj = args->obj;
  Query* query = args->query;
  UsedRelations* ur = NULL;
  int id = args->id;

  if (id==15 || id==29 || id==30 || id==39 || id==53){
    args->output[id] = new char[20];
    sprintf(args->output[id], "%d. ---", id + 1);
    return NULL;
  }
  if (query == NULL){
    args->output[id] = new char[2]{"F"};
    return NULL;
  }

  for (int i = 0; i < query->number_of_predicates; i++){
    /// Priority index
    int idx = query->priority_predicates[i];

    if (obj->bothRelsUsed(ur, query->prdcts[idx]->binding_left, query->prdcts[idx]->binding_right)){
      query->prdcts[idx]->self_join = 1;
    }

    //CASE 1: Join is with filter e.g 1.0 > 3000
    if (query->prdcts[idx]->filter){
      RelColumn* relR = obj->GetUsedRelation(ur, query->prdcts[idx]->relation_left, query->prdcts[idx]->binding_left, query->prdcts[idx]->column_left);
      SingleCol* matches = obj->filterJoin(relR, query->prdcts[idx]->operation, query->prdcts[idx]->number);
      if (ur == NULL) ur = new UsedRelations(matches->activeSize * MAX_MATCHES, query->number_of_relations);
      obj->updateURfilter(ur, query->prdcts[idx]->binding_left, matches);
      delete relR;
      delete matches;
    }
    //CASE 2: Both Relationships are in UR
    else if (query->prdcts[idx]->self_join){
      RelColumn* relR = obj->GetUsedRelation(ur, query->prdcts[idx]->relation_left, query->prdcts[idx]->binding_left, query->prdcts[idx]->column_left);
      RelColumn* relS = obj->GetUsedRelation(ur, query->prdcts[idx]->relation_right, query->prdcts[idx]->binding_right, query->prdcts[idx]->column_right);
      SelfCols* matches = obj->selfJoin(relR, relS);
      if (ur == NULL) ur = new UsedRelations(matches->activeSize * MAX_MATCHES, query->number_of_relations);
      obj->updateURself(ur, query->prdcts[idx]->binding_left, query->prdcts[idx]->binding_right, matches);
      delete relR;
      delete relS;
      delete matches;
    }
    //CASE 3: Join is between 2 relationships e.g 1.0 = 2.1
    else {
      RelColumn* relR = obj->GetUsedRelation(ur, query->prdcts[idx]->relation_left, query->prdcts[idx]->binding_left, query->prdcts[idx]->column_left);
      RelColumn* relS = obj->GetUsedRelation(ur, query->prdcts[idx]->relation_right, query->prdcts[idx]->binding_right, query->prdcts[idx]->column_right);
      PartitionedHashJoin* phj = new PartitionedHashJoin(relR, relS);
      Matches* matches = phj->Solve();
      if (ur == NULL) ur = new UsedRelations(matches->activeSize * MAX_MATCHES, query->number_of_relations);
      obj->updateUsedRelations(ur, matches, query->prdcts[idx]->binding_left, query->prdcts[idx]->binding_right);
      delete relR;
      delete relS;
      delete phj;
      delete matches;
    }
    if (ur->activeSize == 0){
      delete ur;
      ur = NULL;
      break;
    }
  }

  char* result = new char[100];
  char tmp[11];
  sprintf(result, "%d.", id+1);
  for (unsigned i=0; i<query->number_of_projections; ++i){
    if (ur == NULL){
      strcat(result, " NULL");
      continue;
    }
    uint64_t sum = obj->Checksum(ur, query->projections[i]->getRealRelation(), query->projections[i]->getRelationIndex(), query->projections[i]->getColumn());
    sprintf(tmp, " %ld", sum);
    strcat(result, tmp);
  }
  args->output[id] = result;
  delete args;
  delete ur;
  return NULL;
}
//-----------------------------------------------------------------------
uint64_t Joiner::Checksum(UsedRelations* usedRelations, unsigned relationId, unsigned binding, unsigned colId){
  uint64_t sum = 0;
  Relation& rel = GetRelation(relationId);
  for (int i=0; i<usedRelations->size; i++){
    if (usedRelations->matchRows[i] == NULL) continue;
    uint32_t idx = usedRelations->matchRows[i]->arr[binding];
    sum += rel.columns[colId][idx];
  }
  return sum;
}
//-----------------------------------------------------------------------
void Joiner::updateUsedRelations(UsedRelations* usedRelations, Matches* matches, int relRid, int relSid){
  if (matches == NULL || matches->activeSize == 0){
    usedRelations->activeSize = 0;
    return;
  }
  // CASE 1 First Join
  if (usedRelations->firstJoin){
    usedRelations->firstJoin = false;
    updateURFirst(usedRelations, matches, relRid, relSid);
    return;
  }
  int i = getFirstURrow(usedRelations);
  // CASE 2.1: Only one of the Relations has been joined before, the Relation R., or both
  if (usedRelations->matchRows[i]->arr[relRid] != -1){
    updateURonlyR(usedRelations, matches, relRid, relSid);
    return;
  }
  // CASE 2.2: Only one of the Relations has been joined before, the Relation S.
  if (usedRelations->matchRows[i]->arr[relSid] != -1){
    updateURonlyS(usedRelations, matches, relSid, relRid);
    return;
  }
}
//-----------------------------------------------------------------------
void Joiner::updateURFirst(UsedRelations* usedRelations, Matches* matches, int relRid, int relSid){
  for (uint32_t i=0; i < matches->activeSize; i++){
    usedRelations->matchRows[i] = new MatchRow(usedRelations->rowSize);
    usedRelations->matchRows[i]->arr[relRid] = matches->tuples[i]->key;
    usedRelations->matchRows[i]->arr[relSid] = matches->tuples[i]->payload;
  }
  usedRelations->activeSize = matches->activeSize;
}
//-----------------------------------------------------------------------
void Joiner::updateURonlyR(UsedRelations* usedRelations, Matches* matches, int relUR, int relNew){
  UsedRelationsTemp* temp = new UsedRelationsTemp(matches->activeSize * MAX_MATCHES, usedRelations->rowSize);
  uint32_t actives = usedRelations->activeSize;
  bool del;

  for (uint32_t i=0; i < usedRelations->size; i++){ /// For each entry from usedRelations
    if (usedRelations->matchRows[i] == NULL) continue;

    del = true;
    uint32_t rowid = usedRelations->matchRows[i]->arr[relUR];
    for (uint32_t j=0; j < matches->activeSize; j++){ /// Check if exists in new match table

      if (rowid == matches->tuples[j]->key){
        usedRelations->matchRows[i]->arr[relNew] = matches->tuples[j]->payload;
        tempStoreDuplicatesR(usedRelations, j, temp, relNew, matches, rowid, i);
        del = false;
        break;
      }
    }
    if (del){
      delete usedRelations->matchRows[i];
      usedRelations->matchRows[i] = NULL;
      usedRelations->activeSize--;
    }
    if (--actives == 0) break;
  }
  moveUR(usedRelations, temp);
}
//-----------------------------------------------------------------------
void Joiner::updateURonlyS(UsedRelations* usedRelations, Matches* matches, int relUR, int relNew){
  UsedRelationsTemp* temp = new UsedRelationsTemp(matches->activeSize * MAX_MATCHES, usedRelations->rowSize);

  uint32_t actives = usedRelations->activeSize;
  bool del;

  for (uint32_t i=0; i < usedRelations->size; i++){ /// For each entry from usedRelations
    if (usedRelations->matchRows[i] == NULL) continue;

    del = true;
    uint32_t rowid = usedRelations->matchRows[i]->arr[relUR];
    for (uint32_t j=0; j<matches->activeSize; j++){ /// Check if exists in new match table
      if (rowid == matches->tuples[j]->payload){
        usedRelations->matchRows[i]->arr[relNew] = matches->tuples[j]->key;
        tempStoreDuplicatesS(usedRelations, j, temp, relNew, matches, rowid, i);
        del = false;
        break;
      }
    }
    if (del){
      delete usedRelations->matchRows[i];
      usedRelations->matchRows[i] = NULL;
      usedRelations->activeSize--;
    }
    if (--actives == 0) break;
  }
  moveUR(usedRelations, temp);
}
//-----------------------------------------------------------------------
void Joiner::updateURfilter(UsedRelations* usedRelations, int relId, SingleCol* sc){
  if (sc->activeSize == 0){ //NO matches clear UR
    usedRelations->activeSize = 0;
    return;
  }
  if (usedRelations->firstJoin){
    usedRelations->firstJoin = false;
    for (uint32_t i = 0; i < sc->activeSize; i++){
      usedRelations->matchRows[i] = new MatchRow(usedRelations->rowSize);
      usedRelations->matchRows[i]->arr[relId] = sc->arr[i];
    }
    usedRelations->activeSize = sc->activeSize;
    return;
  }

  uint32_t actives = usedRelations->activeSize;
  bool del;
  for (uint32_t i=0; i < usedRelations->size; i++){
    if (usedRelations->matchRows[i] == NULL) continue;

    del = true;
    uint32_t rowid = usedRelations->matchRows[i]->arr[relId];
    for (uint32_t j=0; j < sc->activeSize; j++){ /// Check if exists in new match table
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
    if (--actives == 0) return;
  }
}
void Joiner::updateURself(UsedRelations* usedRelations, int relRid, int relSid, SelfCols* sc){
  if (sc->activeSize == 0){ //NO matches clear UR
    usedRelations->activeSize = 0;
    return;
  }
  if (usedRelations->firstJoin){
    usedRelations->firstJoin = false;
    for (uint32_t i = 0; i < sc->activeSize; i++){
      usedRelations->matchRows[i] = new MatchRow(usedRelations->rowSize);
      usedRelations->matchRows[i]->arr[relRid] = sc->arr[i][0];
      usedRelations->matchRows[i]->arr[relSid] = sc->arr[i][1];

    }
    usedRelations->activeSize = sc->activeSize;
    return;
  }

  uint32_t actives = usedRelations->activeSize;
  bool del;
  for (uint32_t i=0; i < usedRelations->size; i++){
    if (usedRelations->matchRows[i] == NULL) continue;

    del = true;
    uint32_t rowidR = usedRelations->matchRows[i]->arr[relRid];
    uint32_t rowidS = usedRelations->matchRows[i]->arr[relSid];

    for (uint32_t j=0; j < sc->activeSize; j++){ /// Check if exists in new match table
      if (rowidR == sc->arr[j][0] && rowidS == sc->arr[j][1]){
        del = false;
        break;
      }
    }
    if (del){
      delete usedRelations->matchRows[i];
      usedRelations->matchRows[i] = NULL;
      usedRelations->activeSize--;
    }
    if (--actives == 0) return;
  }
}
//-- old self join--------------------
//-----------------------------------------------------------------------
// SingleCol* Joiner::selfJoin(RelColumn* relR, RelColumn* relS){
//   SingleCol* singleCol = new SingleCol(relR->num_tuples);
//   for (uint32_t i=0; i < relR->num_tuples; i++){
//     if (relR->tuples[i].payload == relS->tuples[i].payload){
//       singleCol->arr[singleCol->activeSize] = relR->tuples[i].key;
//       singleCol->activeSize++;
//     }
//   }
//   return singleCol;
// }
//-----------------------------------------------------------------------
SelfCols* Joiner::selfJoin(RelColumn* relR, RelColumn* relS){
  SelfCols* selfCols = new SelfCols(relR->num_tuples);
  for (uint32_t i=0; i < relR->num_tuples; i++){
    if (relR->tuples[i].payload == relS->tuples[i].payload){
      selfCols->arr[selfCols->activeSize][0] = relR->tuples[i].key;
      selfCols->arr[selfCols->activeSize][1] = relS->tuples[i].key;
      selfCols->activeSize++;
    }
  }
  return selfCols;
}
//-----------------------------------------------------------------------
bool Joiner::isSelfJoin(unsigned int relR, unsigned int relS){
  return relR == relS;
}
uint32_t Joiner::getFirstURrow(UsedRelations* usedRelations){
  if (usedRelations == NULL || usedRelations->firstJoin) return -1;
  for (uint32_t i = 0; i < usedRelations->size; i++){
    if (usedRelations->matchRows[i] != NULL){
      return i;
    }
  }
  return -1;
}
//-----------------------------------------------------------------------
bool Joiner::isFilterJoin(char operation){
  return (operation != '=');
}
//-----------------------------------------------------------------------
SingleCol* Joiner::filterJoin(RelColumn* rel, char operation, int n){
  SingleCol* singleCol = new SingleCol(rel->num_tuples);
  if (operation == '>'){
    for (uint32_t i = 0; i < rel->num_tuples; i++){
      if (rel->tuples[i].payload > n){
        singleCol->arr[singleCol->activeSize] = rel->tuples[i].key;
        singleCol->activeSize++;
      }
    }
  }
  else if (operation == '<'){
    for (uint32_t i = 0; i < rel->num_tuples; i++){
      if (rel->tuples[i].payload < n){
        singleCol->arr[singleCol->activeSize] = rel->tuples[i].key;
        singleCol->activeSize++;
      }
    }
  }
  else {
    for (uint32_t i = 0; i < rel->num_tuples; i++){
      if (rel->tuples[i].payload == n){
        singleCol->arr[singleCol->activeSize] = rel->tuples[i].key;
        singleCol->activeSize++;
      }
    }
  }
  return singleCol;
}
//-----------------------------------------------------------------------
void Joiner::PrintUsedRelations(UsedRelations* usedRelations){
  cout << "\n--- Join Results in UR table---\n\n" << endl;
  for (int i=0; i<usedRelations->size; i++){
    if (usedRelations->matchRows[i] != NULL) {
      for (int j = 0; j < usedRelations->matchRows[i]->size; j++) {
        cout << usedRelations->matchRows[i]->arr[j] << " ";
      }
      cout << endl;
    }
  }
}
//-----------------------------------------------------------------------
Joiner::~Joiner(){
  for (int i = 0; i<numRelations; i++){
    delete relations[i];
  }
  delete[] relations;
}
//-----------------------------------------------------------------------
void Joiner::moveUR(UsedRelations* usedRelations, UsedRelationsTemp* temp){
  int prevJ = 0;
  for (int i = 0; i < temp->activeSize; i++){
    for (int j = prevJ; j < usedRelations->size; j++){
      if (usedRelations->matchRows[j] == NULL){
        usedRelations->matchRows[j] = temp->matchRows[i];
        usedRelations->activeSize++;
        break;
      }
      prevJ = j + 1;
    }
  }
  delete temp;
}
//-----------------------------------------------------------------------
void Joiner::tempStoreDuplicatesR(UsedRelations* usedRelations, int j, UsedRelationsTemp* temp, int relNew, Matches* matches, uint32_t rowid, int i){
  for (int k = j + 1; k < matches->activeSize; k++){
    if (rowid == matches->tuples[k]->key){
      temp->matchRows[temp->activeSize] = new MatchRow(usedRelations->rowSize);

      for (int p = 0; p < usedRelations->rowSize; p++){
        temp->matchRows[temp->activeSize]->arr[p] = usedRelations->matchRows[i]->arr[p];
      }
      temp->matchRows[temp->activeSize]->arr[relNew] = matches->tuples[k]->payload;
      temp->activeSize++;
    }
  }
}
//-----------------------------------------------------------------------
void Joiner::tempStoreDuplicatesS(UsedRelations* usedRelations, int j, UsedRelationsTemp* temp, int relNew, Matches* matches, uint32_t rowid, int i){
  for (int k = j + 1; k < matches->activeSize; k++){
    if (rowid == matches->tuples[k]->payload){
      temp->matchRows[temp->activeSize] = new MatchRow(usedRelations->rowSize);

      for (int p = 0; p < usedRelations->rowSize; p++){
        temp->matchRows[temp->activeSize]->arr[p] = usedRelations->matchRows[i]->arr[p];
      }
      temp->matchRows[temp->activeSize]->arr[relNew] = matches->tuples[k]->key;
      temp->activeSize++;
    }
    else return;
  }
}
//-----------------------------------------------------------------------
void Joiner::printMatches(Matches* matches){
  for (int i = 0; i< matches->activeSize; i++){
    cout << matches->tuples[i]->key << " " << matches->tuples[i]->payload << endl;
  }
}
//-----------------------------------------------------------------------
bool Joiner::bothRelsUsed(UsedRelations* usedRelations, int relRid, int relSid){
  int i = getFirstURrow(usedRelations);
  if (i == -1) return false;
  if ((usedRelations->matchRows[i]->arr[relRid] != -1) && (usedRelations->matchRows[i]->arr[relSid]!=-1))
    return true;
  return false;
}
