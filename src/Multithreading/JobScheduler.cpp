#include "JobScheduler.h"

//-----------------------------------------------------------------------
int JobScheduler::initialize_scheduler(int execution_threads){
  this->execution_threads = execution_threads;
  this->q = NULL;
  tids = (pthread_t*)malloc(sizeof(pthread_t) * execution_threads);
  for (int i = 0;i < execution_threads;i++) {
    if(pthread_create(&(tids[i]),NULL,do_work,this)) {
      cout << "[JobScheduler] Thread initiation error\n";
      return -1;
	  }
  }
  return 0;
}
//-----------------------------------------------------------------------
void* do_work(void* object){
  JobScheduler* sch = reinterpret_cast<JobScheduler*>(object);
  cout<<"hi\n";
  return 0;
}
//-----------------------------------------------------------------------
/// Implement
int JobScheduler::submit_job(Job* j){
	if(q->size == 0) { //set to only one
		q->head = j;
		q->tail = j;
	} else {  //apppend to end;
		q->tail->next = j;
		q->tail = j;
	}
	q->size++;
  return 0;
}
//-----------------------------------------------------------------------
/// Implement
int JobScheduler::execute_all_jobs(){
  return 0;
}
//-----------------------------------------------------------------------
int JobScheduler::wait_all_tasks_finish(){
  for (int i = 0; i < this->execution_threads; i++){
    pthread_join(tids[i], NULL);
  }
  return 0;
}
//-----------------------------------------------------------------------
int JobScheduler::destroy_scheduler(){
  delete this->q;
  delete[] this->tids;
  delete this;
  return 0;
}
