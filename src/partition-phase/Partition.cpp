#include "Partition.h"

Partition::Partition(RelColumn* rel, int n, int from, int to){
  this->n = n;
  this->rel = rel;
  this->startIndex = from;
  this->endIndex = to == -1 ? rel->num_tuples : to;
}

uint32_t Partition::Hash(uint32_t key, int n){
  return key & ((1 << n) - 1);
}

Part* Partition::BuildPartitionedTable(){
  Part* part = new Part();
  Hist* hist = CreateHistogram();

  part->prefixSum = CreatePrefixSum(hist);
  part->rel = new RelColumn(endIndex - startIndex);

  uint32_t* indexArr = new uint32_t[part->prefixSum->length]{};
  uint32_t index;

  for (uint32_t i = startIndex; i < endIndex; i++){
    int hash = Hash(rel->tuples[i].payload, n);

    for (int j = 0; j < part->prefixSum->length-1; j++){
      if (part->prefixSum->arr[j][0] == hash){
        index = part->prefixSum->arr[j][1] + indexArr[j]++;
        part->rel->tuples[index] = rel->tuples[i];
        break;
      }
    }
  }
  delete[] indexArr;
  delete hist;
  return part;
}

// Part* Partition::BuildPartitionedTable(){
//   Part* part = new Part();
//   Hist* hist = CreateHistogram();
//   pthread_mutex_t* mtx;
//
//   part->prefixSum = CreatePrefixSum(hist);
//   part->rel = new RelColumn(endIndex - startIndex);
//
//   uint32_t mtxSize = part->prefixSum->length;
//   mtx = new pthread_mutex_t[mtxSize];
//
//   int numThreads = sch.execution_threads;
//   uint32_t offset = floor((endIndex-startIndex) / numThreads);
//   uint32_t end, start = startIndex;
//
//   for (uint32_t i = 0; i < mtxSize; i++)
//     pthread_mutex_init(&mtx[i],NULL);
//
//   for (int t=0; t < numThreads; t++){
//     end = start + offset;
//     if (t == numThreads - 1){ //last thread gets remaining
//       end = endIndex;
//     }
//     sch.submit_job(new Job(thread_BuildPartitionedTable, (void*)new BuildArgs(this, part, mtx, start, end)));
//     start += offset;
//   }
//   sch.wait_all_tasks_finish();
//
//   for (uint32_t i = 0; i < mtxSize; i++)
//     pthread_mutex_destroy(mtx);
//
//   delete [] mtx;
//   delete hist;
//   return part;
// }

void* Partition::thread_BuildPartitionedTable(void* vargp){
  BuildArgs* args = (BuildArgs*)vargp;
  Partition* inst = args->instance;

  for (int i = args->start; i < args->end; i++){
    int hash = inst->Hash(inst->rel->tuples[i].payload, inst->n);
    int index, j;

    for (j = 0; j < args->part->prefixSum->length; j++){
      if (args->part->prefixSum->arr[j][0] == hash){
        index = args->part->prefixSum->arr[j][1];
        break;
      }
    }
    pthread_mutex_lock(&(args->lock[j]));
    for (; args->part->rel->tuples[index].payload != 0; index++); //find empty bucket
    args->part->rel->tuples[index] = inst->rel->tuples[i];
    pthread_mutex_unlock(&(args->lock[j]));
  }
  delete args;
  return NULL;
}

Hist* Partition::CreateHistogram(){
  uint32_t histLength = pow(2,n);
  Hist* hist = new Hist(histLength);

  for (uint32_t i = startIndex; i < endIndex; i++){
    int index = Hash(rel->tuples[i].payload, n);
    hist->arr[index]++;
  }

  for (uint32_t i = 0; i < histLength; i++){ //calculate largestTableSize
    if (hist->arr[i] == 0) continue;
    hist->activeSize++;
    if (hist->arr[i] > largestTableSize)
      largestTableSize = hist->arr[i] * sizeof(Tuple);
  }

  return hist;
}

// Hist* Partition::CreateHistogram(){
//   uint32_t histLength = pow(2,n);
//   Hist* hist = new Hist(histLength);
//   //result array having each thread's hist
//   int numThreads = sch.execution_threads;
//   Hist** histArr = new Hist*[numThreads];
//
//   uint32_t offset = floor((endIndex-startIndex) / numThreads);
//   uint32_t end, start = startIndex;
//
//   for (int t=0; t<numThreads; t++){
//     end = start + offset;
//     if (t == numThreads - 1){ //last thread gets remaining
//       end = endIndex;
//     }
//     sch.submit_job(new Job(thread_CreateHistogram, (void*)new HistArgs(this, histArr, t, start, end)));
//     start += offset;
//   }
//   sch.wait_all_tasks_finish();
//
//   uint32_t val;
//   for (int j=0; j<histLength; j++){ //create a single hist from the resulting ones
//     for (int i=0; i<numThreads; i++){
//       if ((val = histArr[i]->arr[j]) == 0) continue;
//       hist->arr[j] += val;
//     }
//     if ((val = hist->arr[j]) > 0){
//       hist->activeSize++;
//       if (val > largestTableSize)
//         largestTableSize = val;
//     }
//   }
//   largestTableSize *= sizeof(Tuple);
//   for (int t=0; t<numThreads; t++) delete histArr[t];
//   delete[] histArr;
//   return hist;
// }

void* Partition::thread_CreateHistogram(void* vargp){
  HistArgs* args = (HistArgs*)vargp;
  Partition* instance = args->instance;
  Hist* hist = args->histArr[args->thread_id] = new Hist(pow(2,instance->n));

  for (uint32_t i=args->start; i<args->end; i++){
    uint32_t index = instance->Hash(instance->rel->tuples[i].payload, instance->n);
    hist->arr[index]++;
  }
  delete args;
  return NULL;
}

PrefixSum* Partition::CreatePrefixSum(Hist* hist){
  uint32_t psum = 0;
  PrefixSum* prefixSum;

  prefixSum = new PrefixSum(hist->activeSize + 1);

  for (uint32_t i = 0; i < hist->length; i++){
    if (hist->arr[i] == 0)
      continue;
    prefixSum->arr[prefixSum->activeSize][0] = i;
    prefixSum->arr[prefixSum->activeSize++][1] = psum;
    psum += hist->arr[i];
  }
  prefixSum->arr[prefixSum->activeSize][0] = -1;
  prefixSum->arr[prefixSum->activeSize++][1] = psum;

  return prefixSum;
}

uint32_t Partition::GetLargestTableSize(){ return largestTableSize; }
