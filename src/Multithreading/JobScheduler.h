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
  int initialize_scheduler(int execution_threads);
  int submit_job(JobScheduler* sch, Job* j);
  int execute_all_jobs(JobScheduler* sch);
  int wait_all_tasks_finish(JobScheduler* sch);
  int destroy_scheduler(JobScheduler* sch);
};
