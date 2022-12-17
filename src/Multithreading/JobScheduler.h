#include <iostream>
#include <pthread.h>
#include "Job.h"
#include "Queue.h"

#define NUM_THREADS 4;
using namespace std;

class JobScheduler{
private:
  int execution_threads; // number of execution threads
  pthread_t* tids; // execution threads
public:
  bool quit;
  Queue* q; // a queue that holds submitted jobs / tasks
  pthread_mutex_t qlock;		//lock on the queue list
	pthread_cond_t q_not_empty;	 //non empty and empty condidtion variables
	pthread_cond_t q_empty;
  int initialize_scheduler(int);
  int submit_job(Job*);
  int execute_all_jobs();
  int wait_all_tasks_finish();
  int destroy_scheduler();
};

void * do_work(void*);
