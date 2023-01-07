#include "Joiner.hpp"

Joiner::Joiner(uint32_t size){
  this->size = size;
  relations = new Relation*[size]{};
}
//-----------------------------------------------------------------------
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
    char error[126];
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
  MatchRow* firstRow = getFirstURrow();
  if (firstRow == NULL || firstRow->arr[binding] == -1)
    return GetRelationCol(relationId, colId);

  Relation& rel = GetRelation(relationId);
  RelColumn* relColumn = new RelColumn(usedRelations->list.size);
  relColumn->num_tuples = 0;
  for (Node* l=usedRelations->list.head; l->next!=NULL; l=l->next){
    uint32_t rowid = l->next->row->arr[binding];
    relColumn->tuples[relColumn->num_tuples].key = rowid;
    relColumn->tuples[relColumn->num_tuples++].payload = rel.columns[colId][rowid];
  }
  return relColumn;
}
//-----------------------------------------------------------------------
void Joiner::Join(Query& query)
  // Executes a join query
{
  usedRelations = new UsedRelations(query.number_of_relations);

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
      updateURself_Filter(query.prdcts[idx]->binding_left, matches);
      delete relR;
      delete matches;
    }
    //CASE 2: Both Relationships are in UR
    else if (query.prdcts[idx]->self_join){
      RelColumn* relR = GetUsedRelation(query.prdcts[idx]->relation_left, query.prdcts[idx]->binding_left, query.prdcts[idx]->column_left);
      RelColumn* relS = GetUsedRelation(query.prdcts[idx]->relation_right, query.prdcts[idx]->binding_right, query.prdcts[idx]->column_right);
      SingleCol* matches = selfJoin(relR, relS);
      updateURself_Filter(query.prdcts[idx]->binding_left, matches);
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
      updateUsedRelations(matches, query.prdcts[idx]->binding_left, query.prdcts[idx]->binding_right);
      delete relR;
      delete relS;
      delete phj;
      delete matches;
    }
    if (usedRelations == NULL || usedRelations->list.size == 0){
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
}
//-----------------------------------------------------------------------
uint64_t Joiner::Checksum(unsigned relationId, unsigned binding, unsigned colId){
  if (getFirstURrow()->arr[binding] == -1) return 0;
  uint64_t sum = 0;
  Relation& rel = GetRelation(relationId);
  for (Node* l=usedRelations->list.head; l->next!=NULL; l=l->next){
    uint32_t idx = l->next->row->arr[binding];
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
  MatchRow* firstRow = getFirstURrow();
  // CASE 2.1: Only one of the Relations has been joined before, the Relation R., or both
  if (firstRow->arr[relRid] != -1){
    updateURonlyR(matches, relRid, relSid);
    return;
  }
  // CASE 2.2: Only one of the Relations has been joined before, the Relation S.
  if (firstRow->arr[relSid] != -1){
    updateURonlyS(matches, relSid, relRid);
    return;
  }
}
//-----------------------------------------------------------------------
void Joiner::updateURFirst(Matches* matches, int relRid, int relSid){
  uint32_t i;
  for (i=0; i < matches->activeSize; i++){
    MatchRow* newRow = new MatchRow(usedRelations->rowSize);
    newRow->arr[relRid] = matches->tuples[i]->key;
    newRow->arr[relSid] = matches->tuples[i]->payload;
    usedRelations->list.Push(newRow);
  }
}
//-----------------------------------------------------------------------
void Joiner::updateURonlyR(Matches* matches, int relUR, int relNew){
  Node* l = usedRelations->list.head;

  while (l->next!=NULL){ /// For each entry from usedRelations
    Node* n = l->next;
    bool del = true;
    uint32_t rowid = n->row->arr[relUR];
    for (uint32_t j=0; j < matches->activeSize; j++){ /// Check if exists in new match table

      if (rowid == matches->tuples[j]->key){
        n->row->arr[relNew] = matches->tuples[j]->payload;

        /// For all other duplicates
        for (uint32_t k = j + 1; k < matches->activeSize; k++){
          if (rowid == matches->tuples[k]->key){
            MatchRow* newRow = new MatchRow(usedRelations->rowSize);

            for (int p = 0; p < usedRelations->rowSize; p++){
              newRow->arr[p] = n->row->arr[p];
            }
            newRow->arr[relNew] = matches->tuples[k]->payload;
            usedRelations->list.Push(newRow);
          }
        }
        del = false;
        break;
      }
    }
    if (del) usedRelations->list.Delete(l);
    else l = n;
  }
}
//-----------------------------------------------------------------------
void Joiner::updateURonlyS(Matches* matches, int relUR, int relNew){
  Node* l = usedRelations->list.head;

  while (l->next!=NULL){ /// For each entry from usedRelations
    Node* n = l->next;
    bool del = true;
    uint32_t rowid = n->row->arr[relUR];
    for (uint32_t j=0; j < matches->activeSize; j++){ /// Check if exists in new match table

      if (rowid == matches->tuples[j]->payload){
        n->row->arr[relNew] = matches->tuples[j]->key;

        /// For all other duplicates
        for (uint32_t k = j + 1; k < matches->activeSize; k++){
          if (rowid == matches->tuples[k]->payload){
            MatchRow* newRow = new MatchRow(usedRelations->rowSize);

            for (int p = 0; p < usedRelations->rowSize; p++){
              newRow->arr[p] = n->row->arr[p];
            }
            newRow->arr[relNew] = matches->tuples[k]->key;
            usedRelations->list.Push(newRow);
          }
        }
        del = false;
        break;
      }
    }
    if (del) usedRelations->list.Delete(l);
    else l = n;
  }
}
//-----------------------------------------------------------------------
void Joiner::updateURself_Filter(int relId, SingleCol* sc){
  if (sc->activeSize == 0){ //NO matches clear UR
    clearUsedRelations();
    return;
  }
  if (firstJoin){
    firstJoin = false;
    uint32_t i;
    for (i = 0; i < sc->activeSize; i++){
      MatchRow* newRow = new MatchRow(usedRelations->rowSize);
      newRow->arr[relId] = sc->arr[i];
      usedRelations->list.Push(newRow);
    }
    return;
  }
  Node* l=usedRelations->list.head;

  while(l->next!=NULL){ /// For each entry from usedRelations

    bool del = true;
    uint32_t rowid = l->next->row->arr[relId];
    for (uint32_t j=0; j < sc->activeSize; j++){ /// Check if exists in new match table
      if (rowid == sc->arr[j]){
        del = false;
        break;
      }
    }
    if (del) usedRelations->list.Delete(l);
    else l=l->next;
  }
}
//-----------------------------------------------------------------------
SingleCol* Joiner::selfJoin(RelColumn* relR, RelColumn* relS){
  SingleCol* singleCol = new SingleCol(relR->num_tuples);
  for (uint32_t i=0; i<relR->num_tuples; i++){
    if (relR->tuples[i].payload == relS->tuples[i].payload){
      singleCol->arr[singleCol->activeSize] = relR->tuples[i].key;
      singleCol->activeSize++;
    }
  }
  return singleCol;
}
//-----------------------------------------------------------------------
bool Joiner::isSelfJoin(unsigned int relR, unsigned int relS){
  return relR == relS;
}
MatchRow* Joiner::getFirstURrow(){
  if (firstJoin || usedRelations->list.size == 0)
    return NULL;
  return usedRelations->list.head->next->row;
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
bool Joiner::bothRelsUsed(int relRid, int relSid){
  MatchRow* row = getFirstURrow();
  if (row == NULL) return false;
  if ((row->arr[relRid] != -1) && (row->arr[relSid]!=-1))
    return true;
  return false;
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
  for (int i = 0; i<size; i++){
    delete relations[i];
  }
  delete[] relations;
}
//-----------------------------------------------------------------------
void Joiner::printMatches(Matches* matches){
  for (int i = 0; i< matches->activeSize; i++){
    cout << matches->tuples[i]->key << " " << matches->tuples[i]->payload << endl;
  }
}
//-----------------------------------------------------------------------
void Joiner::PrintUsedRelations(){
  cout << "\n--- Join Results in UR table---\n\n" << endl;
  for (Node* l=usedRelations->list.head; l->next!=NULL; l=l->next){ /// For each entry from usedRelations
    for (int j = 0; j < usedRelations->rowSize; j++) {
      cout << l->next->row->arr[j] << " ";
    }
    cout << endl;
  }
}
