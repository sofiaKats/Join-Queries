#include "JobScheduler.h"

//-----------------------------------------------------------------------
JobScheduler sch;
//-----------------------------------------------------------------------
JobScheduler::JobScheduler(){ initialize_scheduler(NUM_THREADS); }
//-----------------------------------------------------------------------
int JobScheduler::initialize_scheduler(int execution_threads){
  this->execution_threads = execution_threads;
  this->q = new Queue();
  this->quit = false;
  this->wait_all = false;
  tids = new pthread_t[execution_threads];
  //initialize mutex and condition variables.
  if(pthread_mutex_init(&qlock,NULL)){
    fprintf(stderr, "[JobScheduler] Mutex initiation error\n");
    return -1;
  }
  if(pthread_cond_init(&q_empty, NULL)){
    fprintf(stderr, "[JobScheduler] CV initiation error\n");
    return -1;
  }
  if(pthread_cond_init(&q_not_empty, NULL)){
    fprintf(stderr, "[JobScheduler] CV initiation error\n");
    return -1;
  }
  for (int i=0; i< execution_threads; i++){
    if(pthread_create(&(tids[i]), NULL, do_work, this)){
      fprintf(stderr, "[JobScheduler] Thread initiation error\n");
      return -1;
	  }
  }
  return 0;
}
//-----------------------------------------------------------------------
void* JobScheduler::do_work(void* object){
  JobScheduler* sch = reinterpret_cast<JobScheduler*>(object);
  Job* j;
	while(1){
		pthread_mutex_lock(&(sch->qlock));

		while(sch->q->size <= 0){ //while queue empty
      if(sch->wait_all){
        sch->counter++;
			}
      if(sch->quit){
				pthread_mutex_unlock(&(sch->qlock));
				pthread_exit(NULL);
			}
			//wait until the condition says queue non-emtpy.
      if(sch->q->size > 0) continue;
      pthread_cond_wait(&(sch->q_not_empty),&(sch->qlock));
		}
		j = sch->q->head;	//set the job variable j.
		sch->q->size--;		//decrement queue size.

		if(sch->q->size == 0){
			sch->q->head = NULL;
			sch->q->tail = NULL;
      pthread_cond_signal(&(sch->q_empty));
		}
		else{
			sch->q->head = j->next;
		}
		pthread_mutex_unlock(&(sch->qlock));
		(j->routine) (j->arg); //start routine
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
	}else {  //apppend to end;
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
  pthread_mutex_lock(&qlock);
  while(q->size > 0){ //wait until the queue is empty.
		pthread_cond_wait(&q_empty,&qlock);
	}
  this->wait_all = true;
  this->counter = 0;
  pthread_cond_broadcast(&q_not_empty);
  pthread_mutex_unlock(&qlock);

  while(1){
    pthread_mutex_lock(&qlock);
    if(counter >= execution_threads)
      break;
    pthread_mutex_unlock(&qlock);
  }
  this->wait_all = false;
  pthread_mutex_unlock(&qlock);

  return 0;
}
//-----------------------------------------------------------------------
int JobScheduler::destroy_scheduler(){
  pthread_mutex_lock(&qlock);
  while(q->size > 0){ //wait until the queue is empty.
		pthread_cond_wait(&q_empty,&qlock);
	}
  this->quit = true;
  pthread_cond_broadcast(&q_not_empty);
  pthread_mutex_unlock(&qlock);
  for(int i=0; i<execution_threads; i++){
		pthread_join(tids[i], NULL);
	}
  pthread_mutex_destroy(&qlock);
	pthread_cond_destroy(&q_empty);
  pthread_cond_destroy(&q_not_empty);
  delete this->q;
  delete[] this->tids;
  return 0;
}
