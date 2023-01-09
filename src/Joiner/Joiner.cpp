#include "Joiner.hpp"

Joiner::Joiner(uint32_t size){
  relations = new Relation*[size]{};
}
//-----------------------------------------------------------------------
void Joiner::AddRelation(const char* fileName)
// Loads a relation from disk
{
    relations[numRelations++] = new Relation(fileName);
}
//---------------------------------------------------------------------------
Relation& Joiner::GetRelation(unsigned relationId)
// Loads a relation from disk
{
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
RelColumn* Joiner::GetUsedRelation(unsigned relationId, unsigned binding, unsigned colId){
  int firstRow = getFirstURrow();
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
string Joiner::Join(Query& query)
  // Executes a join query
{
  for (int i = 0; i < query.number_of_predicates; i++){
    /// Priority index
    int idx = query.priority_predicates[i];

    if (bothRelsUsed(query.prdcts[idx]->binding_left, query.prdcts[idx]->binding_right)){
      query.prdcts[idx]->self_join = 1;
    }

    //CASE 1: Join is with filter e.g 1.0 > 3000
    if (query.prdcts[idx]->filter){
      RelColumn* relR = GetUsedRelation(query.prdcts[idx]->relation_left, query.prdcts[idx]->binding_left, query.prdcts[idx]->column_left);
      SingleCol* matches = filterJoin(relR, query.prdcts[idx]->operation, query.prdcts[idx]->number);
      if (firstJoin) usedRelations = new UsedRelations(matches->activeSize * MAX_NEI_SIZE, query.number_of_relations);
      updateURfilter(query.prdcts[idx]->binding_left, matches);
      delete relR;
      delete matches;
    }
    //CASE 2: Both Relationships are in UR
    else if (query.prdcts[idx]->self_join){
      RelColumn* relR = GetUsedRelation(query.prdcts[idx]->relation_left, query.prdcts[idx]->binding_left, query.prdcts[idx]->column_left);
      RelColumn* relS = GetUsedRelation(query.prdcts[idx]->relation_right, query.prdcts[idx]->binding_right, query.prdcts[idx]->column_right);
      SelfCols* matches = selfJoin(relR, relS);
      if (firstJoin) usedRelations = new UsedRelations(matches->activeSize * MAX_NEI_SIZE, query.number_of_relations);
      updateURself(query.prdcts[idx]->binding_left, query.prdcts[idx]->binding_right, matches);
      delete relR;
      delete relS;
      delete matches;
    }
    //CASE 3: Join is between 2 relationships e.g 1.0 = 2.1
    else {
      RelColumn* relR = GetUsedRelation(query.prdcts[idx]->relation_left, query.prdcts[idx]->binding_left, query.prdcts[idx]->column_left);
      RelColumn* relS = GetUsedRelation(query.prdcts[idx]->relation_right, query.prdcts[idx]->binding_right, query.prdcts[idx]->column_right);
      PartitionedHashJoin* phj = new PartitionedHashJoin(relR, relS);
      Matches* matches = phj->Solve();
      if (firstJoin) usedRelations = new UsedRelations(matches->activeSize * MAX_NEI_SIZE, query.number_of_relations);
      updateUsedRelations(matches, query.prdcts[idx]->binding_left, query.prdcts[idx]->binding_right);
      delete relR;
      delete relS;
      delete phj;
      delete matches;
    }

    if (usedRelations == NULL || usedRelations->activeSize == 0){
      clearJoinSession();
      break;
    }
  }
  for (unsigned i=0; i<query.number_of_projections; ++i){
    if (usedRelations == NULL){
      cout << "NULL ";
      continue;
    }
    uint64_t sum = Checksum(query.projections[i]->getRealRelation(), query.projections[i]->getRelationIndex(), query.projections[i]->getColumn());
    cout << (sum==-1?"NULL":to_string(sum)) << " ";
  }
  cout << "\n";
  clearJoinSession();
  return "";
}
//-----------------------------------------------------------------------
uint64_t Joiner::Checksum(unsigned relationId, unsigned binding, unsigned colId){
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
void Joiner::updateUsedRelations(Matches* matches, int relRid, int relSid){
  if (matches == NULL || matches->activeSize == 0){
    clearUsedRelations();
    return;
  }
  // CASE 1 First Join
  if (firstJoin){
    firstJoin = false;
    updateURFirst(matches, relRid, relSid);
    return;
  }
  int i = getFirstURrow();
  // CASE 2.1: Only one of the Relations has been joined before, the Relation R., or both
  if (usedRelations->matchRows[i]->arr[relRid] != -1){
    updateURonlyR(matches, relRid, relSid);
    return;
  }
  // CASE 2.2: Only one of the Relations has been joined before, the Relation S.
  if (usedRelations->matchRows[i]->arr[relSid] != -1){
    updateURonlyS(matches, relSid, relRid);
    return;
  }
}
//-----------------------------------------------------------------------
void Joiner::updateURFirst(Matches* matches, int relRid, int relSid){
  for (uint32_t i=0; i < matches->activeSize; i++){
    usedRelations->matchRows[i] = new MatchRow(usedRelations->rowSize);
    usedRelations->matchRows[i]->arr[relRid] = matches->tuples[i]->key;
    usedRelations->matchRows[i]->arr[relSid] = matches->tuples[i]->payload;
  }
  usedRelations->activeSize = matches->activeSize;
}
//-----------------------------------------------------------------------
void Joiner::updateURonlyR(Matches* matches, int relUR, int relNew){
  UsedRelationsTemp* temp = new UsedRelationsTemp(matches->activeSize * MAX_NEI_SIZE, usedRelations->rowSize);
  uint32_t actives = usedRelations->activeSize;
  bool del;

  for (uint32_t i=0; i < usedRelations->size; i++){ /// For each entry from usedRelations
    if (usedRelations->matchRows[i] == NULL) continue;

    del = true;
    uint32_t rowid = usedRelations->matchRows[i]->arr[relUR];
    for (uint32_t j=0; j < matches->activeSize; j++){ /// Check if exists in new match table

      if (rowid == matches->tuples[j]->key){
        usedRelations->matchRows[i]->arr[relNew] = matches->tuples[j]->payload;
        tempStoreDuplicatesR(j, temp, relNew, matches, rowid, i);
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
  moveUR(temp);
}
//-----------------------------------------------------------------------
void Joiner::updateURonlyS(Matches* matches, int relUR, int relNew){
  UsedRelationsTemp* temp = new UsedRelationsTemp(matches->activeSize * MAX_NEI_SIZE, usedRelations->rowSize);

  uint32_t actives = usedRelations->activeSize;
  bool del;

  for (uint32_t i=0; i < usedRelations->size; i++){ /// For each entry from usedRelations
    if (usedRelations->matchRows[i] == NULL) continue;

    del = true;
    uint32_t rowid = usedRelations->matchRows[i]->arr[relUR];
    for (uint32_t j=0; j<matches->activeSize; j++){ /// Check if exists in new match table
      if (rowid == matches->tuples[j]->payload){
        usedRelations->matchRows[i]->arr[relNew] = matches->tuples[j]->key;
        tempStoreDuplicatesS(j, temp, relNew, matches, rowid, i);
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
  moveUR(temp);
}
//-----------------------------------------------------------------------
void Joiner::updateURfilter(int relId, SingleCol* sc){
  if (sc->activeSize == 0){ //NO matches clear UR
    clearUsedRelations();
    return;
  }
  if (firstJoin){
    firstJoin = false;
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
void Joiner::updateURself(int relRid, int relSid, SelfCols* sc){
  if (sc->activeSize == 0){ //NO matches clear UR
    clearUsedRelations();
    return;
  }
  if (firstJoin){
    firstJoin = false;
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
uint32_t Joiner::getFirstURrow(){
  if (firstJoin) return -1;
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
void Joiner::PrintUsedRelations(){
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
void Joiner::clearJoinSession(){
  clearUsedRelations();
  firstJoin = true;
}
//-----------------------------------------------------------------------
void Joiner::clearUsedRelations(){
  delete usedRelations;
  usedRelations = NULL;
}
//-----------------------------------------------------------------------
Joiner::~Joiner(){
  for (int i = 0; i<numRelations; i++){
    delete relations[i];
  }
  delete[] relations;
}
//-----------------------------------------------------------------------
void Joiner::moveUR(UsedRelationsTemp* temp){
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
void Joiner::tempStoreDuplicatesR(int j, UsedRelationsTemp* temp, int relNew, Matches* matches, uint32_t rowid, int i){
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
void Joiner::tempStoreDuplicatesS(int j, UsedRelationsTemp* temp, int relNew, Matches* matches, uint32_t rowid, int i){
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
bool Joiner::bothRelsUsed(int relRid, int relSid){
  int i = getFirstURrow();
  if (i == -1) return false;
  if ((usedRelations->matchRows[i]->arr[relRid] != -1) && (usedRelations->matchRows[i]->arr[relSid]!=-1))
    return true;
  return false;
}
