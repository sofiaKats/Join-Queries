#include "JobScheduler.h"

//-----------------------------------------------------------------------
int JobScheduler::initialize_scheduler(int execution_threads){
  this->execution_threads = execution_threads;
  this->q = new Queue();
  this->quit = false;
  tids = (pthread_t*)malloc(sizeof(pthread_t) * execution_threads);
  //initialize mutex and condition variables.
  if(pthread_mutex_init(&qlock,NULL)) {
    fprintf(stderr, "[JobScheduler] Mutex initiation error\n");
	   return -1;
  }
  if(pthread_cond_init(&q_empty,NULL)) {
    fprintf(stderr, "[JobScheduler] CV initiation error\n");
	   return -1;
  }
  if(pthread_cond_init(&q_not_empty,NULL)) {
    fprintf(stderr, "[JobScheduler] CV initiation error\n");
	   return -1;
  }
  for (int i = 0;i < execution_threads;i++) {
    if(pthread_create(&(tids[i]),NULL,do_work,this)) {
      fprintf(stderr, "[JobScheduler] Thread initiation error\n");
      return -1;
	  }
  }
  return 0;
}
//-----------------------------------------------------------------------
void* do_work(void* object){
  JobScheduler* sch = reinterpret_cast<JobScheduler*>(object);
  Job* j;
	while(1){
		pthread_mutex_lock(&(sch->qlock));  //get the q lock.

		while(sch->q->size == 0) {	//if the size is 0 then wait.
			//wait until the condition says its no emtpy and give up the lock.
			pthread_mutex_unlock(&(sch->qlock));  //get the qlock.
			pthread_cond_wait(&(sch->q_not_empty),&(sch->qlock));
      if(sch->quit){
				pthread_mutex_unlock(&(sch->qlock));
				pthread_exit(NULL);
			}
		}
		j = sch->q->head;	//set the job variable j.
		sch->q->size--;		//decriment the size.

		if(sch->q->size == 0){
			sch->q->head = NULL;
			sch->q->tail = NULL;
		}
		else{
			sch->q->head = j->next;
		}

		if(sch->q->size == 0){
			//the q is empty, signal that its empty.
			pthread_cond_signal(&(sch->q_empty));
		}
		pthread_mutex_unlock(&(sch->qlock));
		(j->routine) (j->arg);   //actually do work.
		delete j;
  }
}
//-----------------------------------------------------------------------
int JobScheduler::submit_job(Job* j){
  pthread_mutex_lock(&qlock);
	if(q->size == 0) { //add first
		q->head = j;
		q->tail = j;
    pthread_cond_signal(&q_not_empty);
	} else {  //apppend to end;
		q->tail->next = j;
		q->tail = j;
	}
	q->size++;
  pthread_mutex_unlock(&qlock);
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
  this->quit = true;
  while(q->size != 0){ //wait until the q is empty.
		pthread_cond_wait(&q_empty,&qlock);
	}
  pthread_cond_broadcast(&q_not_empty);
	pthread_mutex_unlock(&qlock);
  for(int i; i < execution_threads;i++){
		pthread_join(tids[i], NULL);
    cout << "joined\n";
	}
  delete this->q;
  delete[] this->tids;
  pthread_mutex_destroy(&qlock);
	pthread_cond_destroy(&q_empty);
	pthread_cond_destroy(&q_not_empty);
  delete this;
  return 0;
}
