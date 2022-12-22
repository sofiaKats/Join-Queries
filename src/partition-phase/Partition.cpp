#include "Partition.h"

Partition::Partition(RelColumn* rel, int n, int from, int to){
  this->n = n;
  this->rel = rel;
  this->startIndex = from;
  this->endIndex = to == -1 ? rel->num_tuples : to;
}

uint32_t Partition::Hash(uint32_t key, int n){
  uint32_t tmp = key << (32 - n);
  return tmp >> (32 - n);
}

Part* Partition::BuildPartitionedTable(){
  Part* part = new Part();
  Hist* hist = CreateHistogram();

  part->prefixSum = CreatePrefixSum(hist);
  part->rel = new RelColumn(endIndex - startIndex);

  int numThreads = sch.execution_threads;
  uint32_t offset = floor((endIndex-startIndex) / numThreads);
  uint32_t end, start = startIndex;

  for (int t=0; t<numThreads; t++){
    end = start + offset;
    if (t == numThreads - 1){ //last thread gets remaining
      end = endIndex;
    }
    sch.submit_job(new Job(thread_BuildPartitionedTable, (void*)new BuildArgs(this, part, t, start, end)));
    start += offset;
  }
  sch.wait_all_tasks_finish();

  return part;
}

void* Partition::thread_BuildPartitionedTable(void* vargp){
  BuildArgs* args = (BuildArgs*)vargp;
  Partition* inst = args->instance;

  for (int i = args->start; i < args->end; i++){
    int hash = inst->Hash(inst->rel->tuples[i].payload, inst->n);
    int index;

    for (int j = 0; j < args->part->prefixSum->length; j++){
      if (args->part->prefixSum->arr[j][0] == hash){
        index = args->part->prefixSum->arr[j][1];
        break;
      }
    }

    for (; args->part->rel->tuples[index].payload != 0; index++); //find empty bucket
    args->part->rel->tuples[index] = inst->rel->tuples[i];
  }
  return 0;
}

/*Hist* Partition::CreateHistogram(){
  int histLength = pow(2,n);
  Hist* hist = new Hist(histLength);

  for (int i = startIndex; i < endIndex; i++){
    int index = Hash(rel->tuples[i].payload, n);
    hist->arr[index]++;
  }

  for (int i = 0; i < histLength; i++){ //calculate largestTableSize
    if (hist->arr[i] == 0) continue;
    hist->activeSize++;
    if (hist->arr[i] > largestTableSize)
      largestTableSize = hist->arr[i] * sizeof(Tuple);
  }

  return hist;
}*/

Hist* Partition::CreateHistogram(){
  uint32_t histLength = pow(2,n);
  Hist* hist = new Hist(histLength);
  //result array having each thread's hist
  int numThreads = sch.execution_threads;
  Hist** histArr = new Hist*[numThreads];

  uint32_t offset = floor((endIndex-startIndex) / numThreads);
  uint32_t end, start = startIndex;

  for (int t=0; t<numThreads; t++){
    end = start + offset;
    if (t == numThreads - 1){ //last thread gets remaining
      end = endIndex;
    }
    sch.submit_job(new Job(thread_CreateHistogram, (void*)new HistArgs(this, histArr, t, start, end)));
    start += offset;
  }
  sch.wait_all_tasks_finish();

  uint32_t val;
  for (int j=0; j<histLength; j++){ //create a single hist from the resulting ones
    for (int i=0; i<numThreads; i++){
      if ((val = histArr[i]->arr[j]) == 0) continue;
      hist->arr[j] += val;
    }
    if ((val = hist->arr[j]) > 0){
      hist->activeSize++;
      if (val > largestTableSize)
        largestTableSize = val * sizeof(Tuple);
    }
  }
  return hist;
}

void* Partition::thread_CreateHistogram(void* vargp){
  HistArgs* args = (HistArgs*)vargp;
  Partition* instance = args->instance;
  Hist* hist = args->histArr[args->thread_id] = new Hist(pow(2,instance->n));

  for (uint32_t i=args->start; i<args->end; i++){
    uint32_t index = instance->Hash(instance->rel->tuples[i].payload, instance->n);
    hist->arr[index]++;
  }
  delete args;
  return 0;
}

PrefixSum* Partition::CreatePrefixSum(Hist* hist){
  uint32_t psum = 0;
  uint32_t pIndex = 0;
  PrefixSum* prefixSum;

  prefixSum = new PrefixSum(hist->activeSize + 1);

  for (uint32_t i = 0; i < hist->length; i++){
    if (hist->arr[i] == 0)
      continue;

    prefixSum->arr[pIndex][0] = i;
    prefixSum->arr[pIndex++][1] = psum;
    psum += hist->arr[i];
  }
  prefixSum->arr[pIndex][0] = -1;
  prefixSum->arr[pIndex][1] = psum;

  return prefixSum;
}

uint32_t Partition::GetLargestTableSize(){
  return largestTableSize;
}
