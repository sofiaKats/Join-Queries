#include <iostream>
#include <pthread.h>
#include "Job.h"
#include "Queue.h"

#define NUM_THREADS 4;
using namespace std;

class JobScheduler{
private:
  int execution_threads; // number of execution threads
  Queue* q; // a queue that holds submitted jobs / tasks
  pthread_t* tids; // execution threads
  // +++ mutex, condition variable, .
public:
  int initialize_scheduler(int);
  int submit_job(Job*);
  int execute_all_jobs();
  int wait_all_tasks_finish();
  int destroy_scheduler();
};

void * do_work(void*);
