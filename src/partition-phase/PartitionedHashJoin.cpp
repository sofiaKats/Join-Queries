#include "PartitionedHashJoin.h"
#include "inttypes.h"

#define L2CACHE 256000

PartitionedHashJoin::PartitionedHashJoin(RelColumn* relR, RelColumn* relS){
  this->relR = relR;
  this->relS = relS;
}

Matches* PartitionedHashJoin::Solve(){
  Matches* matches;
  Part *partitionedS = NULL, *partitionedR = NULL;
  int passCount = PartitionRec(&partitionedR, relR->num_tuples, relR);

  try{
    BuildHashtables(partitionedR);
    PartitionRec(&partitionedS, relS->num_tuples, relS, passCount);
    matches = Join(partitionedR, partitionedS);

  }
  catch(const exception &e){
    cout << e.what() << endl;
  }

  delete partitionedR;
  delete partitionedS;
  return matches;
}

void PartitionedHashJoin::Merge(Part** destPart, uint32_t relSize, Part* part, uint32_t from, int n, int passNum){
  if (passNum == 1) { *destPart = part; return; }
  uint32_t partIndex = 0;
  uint32_t index = 0;
  uint32_t base = 0;

  if (*destPart == NULL){
    *destPart = new Part();
    (*destPart)->rel = new RelColumn(relSize);
    (*destPart)->prefixSum = new PrefixSum(pow(2, n) + 1);
  }
  //Merge Relation table
  for (int i = from; i < from + part->rel->num_tuples; i++){
   (*destPart)->rel->tuples[i] = part->rel->tuples[partIndex++];
  }
  //Merge PrefixSum table
  index = (*destPart)->prefixSum->activeSize;
  index = index == 0 ? index : --(*destPart)->prefixSum->activeSize;
  base = (*destPart)->prefixSum->arr[index][1];

  for (int i = 0; i < part->prefixSum->activeSize; i++){
   (*destPart)->prefixSum->arr[i+index][0] = part->prefixSum->arr[i][0];
   (*destPart)->prefixSum->arr[i+index][1] = part->prefixSum->arr[i][1] + base;
   (*destPart)->prefixSum->activeSize++;
  }
  delete part;
}

int PartitionedHashJoin::PartitionRec(Part** finalPart, uint32_t finalSize, RelColumn* rel, int maxPasses, int n, int passNum, uint32_t from, uint32_t to){
  passNum++;
  n++;
  int passCount = 0;

  Partition* partition = new Partition(rel, n, from, to);
  Part* part = partition->BuildPartitionedTable();

  if (passNum == maxPasses || partition->GetLargestTableSize() < L2CACHE){
    //Merge Relation and PrefixSum table to a final Part
    Merge(finalPart, finalSize, part, from, n, passNum);
    delete partition;
    return passNum;
  }
  for (uint32_t i = 0; i < part->prefixSum->length - 1; i++){
    from = part->prefixSum->arr[i][1];
    to = part->prefixSum->arr[i+1][1];
    passCount = PartitionRec(finalPart, finalSize, part->rel, maxPasses, n, passNum, from, to);
  }

  delete partition;
  delete part;

  return passCount;
}

void PartitionedHashJoin::BuildHashtables(Part* part){
  uint32_t hashtablesLength = part->prefixSum->activeSize;
  uint32_t partitionSize;
  uint32_t indexR = 0;

  part->hashtables = new Hashtable*[hashtablesLength];
  //for every partition table
  for (uint32_t i = 1; i < hashtablesLength; i++){
    //if(part->prefixSum->arr[i-1][0] == -1) return;

    //find its size from prefix sum
    partitionSize = part->prefixSum->arr[i][1] - part->prefixSum->arr[i - 1][1];

    //create new hashtable for this partition
    part->hashtables[i - 1] = new Hashtable(partitionSize);

    //fill hashtable
    for (uint32_t j = 0; j < partitionSize; j++){
      part->hashtables[i - 1]->add(part->rel->tuples[indexR].payload, part->rel->tuples[indexR].key);
      indexR++;
    }
  }
}

Matches* PartitionedHashJoin::Join(Part* p1, Part* p2){
  /// Build final array of tuples containing matching rowids
  uint32_t matchesSize = (p1->rel->num_tuples > p2->rel->num_tuples ? p2->rel->num_tuples : p1->rel->num_tuples) * MAX_MATCHES;
  Matches* final = new Matches(matchesSize);

  pthread_mutex_t mtx;
  pthread_mutex_init(&mtx,NULL);

  // For every partition table
  for (int i = 0; i < p2->prefixSum->length; i++){
    if (p2->prefixSum->arr[i][0] == -1) break;
    int hash = p2->prefixSum->arr[i][0];
    int hashIndex = ExistsInPrefix(hash, p1->prefixSum);
    if (hashIndex != -1) // If hash value exists in relation R
      sch2.submit_job(new Job(thread_Join, (void*)new JoinArgs(final, p1, p2, hashIndex, i, &mtx)));
  }
  sch2.wait_all_tasks_finish();
  pthread_mutex_destroy(&mtx);

  return final;
}

void* PartitionedHashJoin::thread_Join(void* vargp){
  //cout << "Thread id in join is " <<  pthread_self() << endl;

  JoinArgs* args = (JoinArgs*)vargp;
  Part* p1 = args->part1;
  Part* p2 = args->part2;
  Hashtable* hashtable = p1->hashtables[args->hashi];

  for (uint32_t j = p2->prefixSum->arr[args->i][1]; j < p2->prefixSum->arr[args->i+1][1]; j++){
    Tuple* tuple = new Tuple(p2->rel->tuples[j].key, p2->rel->tuples[j].payload);
    //cout << p2->rel->tuples[j].key << " " << p2->rel->tuples[j].payload << endl;
    MatchesPtr* matches = hashtable->contains(tuple);

    pthread_mutex_lock(args->lock);
    for (uint32_t k = 0; k < matches->activeSize; k++)
      args->matches->tuples[args->matches->activeSize++] = matches->tuples[k];
    pthread_mutex_unlock(args->lock);

    delete matches;
    delete tuple;
  }
  delete args;
  return NULL;
}
// Matches* PartitionedHashJoin::Join(Part* p1, Part* p2){
//   int hashtablesIndex;

//   /// Build final array of tuples containing matching rowids
//   uint32_t matchesSize = (p1->rel->num_tuples > p2->rel->num_tuples ? p2->rel->num_tuples : p1->rel->num_tuples) * MAX_MATCHES;
//   Matches* final = new Matches(matchesSize);

//   // For every partition table
//   for (int i = 0; i < p2->prefixSum->length; i++){
//     if (p2->prefixSum->arr[i][0] == -1) break;

//     int hash = p2->prefixSum->arr[i][0];

//     // If hash value exists in relation R
//     hashtablesIndex = ExistsInPrefix(hash, p1->prefixSum);

//     if (hashtablesIndex != -1){
//       // For every tuple in this partition
//       for (uint32_t j = p2->prefixSum->arr[i][1]; j < p2->prefixSum->arr[i+1][1]; j++){
//         Tuple* tuple = new Tuple(p2->rel->tuples[j].key, p2->rel->tuples[j].payload);
//         MatchesPtr* matches = p1->hashtables[hashtablesIndex]->contains(tuple);

//         for (uint32_t k = 0; k < matches->activeSize; k++)
//           final->tuples[final->activeSize++] = matches->tuples[k];

//         delete matches;
//         delete tuple;
//       }
//     }
//   }
//   return final;
// }

int PartitionedHashJoin::ExistsInPrefix(int hash, PrefixSum* prefixSum){
  for (int i = 0; i < prefixSum->length; i++){
    if (prefixSum->arr[i][0] == hash) return i;
    if (prefixSum->arr[i][0] == -1) return -1;
  }
  return -1;
}

void PartitionedHashJoin::PrintHashtables(Part* part){
  for (int i = 1 ; i < part->prefixSum->length; i++){
    cout << "\n\nHASHTABLE NUMBER: " << i - 1 << endl;
    part->hashtables[i - 1]->print_hashtable();

    if(part->prefixSum->arr[i][0] == -1) break;
  }
}

void PartitionedHashJoin::PrintRelation(RelColumn* rel){
  cout << "\n----- Relation Table -----\n";
  for (int i = 0 ; i < rel->num_tuples; i++){
    cout << rel->tuples[i].payload << endl;
  }
}

void PartitionedHashJoin::PrintPrefix(PrefixSum* prefixSum){
  cout << "\n----- PrefixSum Table -----\n";
  for (int i = 0 ; i < prefixSum->length; i++){
    if (prefixSum->arr[i][0] == -1){
      cout << prefixSum->arr[i][0] << " : " << prefixSum->arr[i][1] << endl;
      break;
    }
    cout << prefixSum->arr[i][0] << " : " << prefixSum->arr[i][1] << endl;
  }
}

void PartitionedHashJoin::PrintPart(Part* part, bool hasHashtables){
  PrintRelation(part->rel);
  PrintPrefix(part->prefixSum);
  if (hasHashtables) PrintHashtables(part);
}
