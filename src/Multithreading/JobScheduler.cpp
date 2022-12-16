#include "JobScheduler.h"

void *work(void *vargp){
    cout << "worked\n";
    return 0;
}
//-----------------------------------------------------------------------
int JobScheduler::initialize_scheduler(int execution_threads){
  this->execution_threads = execution_threads;
  tids = (pthread_t*)malloc(sizeof(pthread_t) * execution_threads);
  for (int i = 0;i < execution_threads;i++) {
    if(pthread_create(&(tids[i]),NULL,work,NULL)) {
      cout << "[JobScheduler] Thread initiation error\n";
      return -1;
	  }
  }
  return 0;
}
//-----------------------------------------------------------------------
/// Implement
int JobScheduler::submit_job(JobScheduler* sch, Job* j){
  return 0;
}
//-----------------------------------------------------------------------
/// Implement
int JobScheduler::execute_all_jobs(JobScheduler* sch){
  return 0;
}
//-----------------------------------------------------------------------
int JobScheduler::wait_all_tasks_finish(JobScheduler* sch){
  for (int i = 0; i < sch->execution_threads; i++){
    pthread_join(tids[i], NULL);
  }
  return 0;
}
//-----------------------------------------------------------------------
int JobScheduler::destroy_scheduler(JobScheduler* sch){
  //delete q;
  delete[] sch->tids;
  delete sch;
  return 0;
}
