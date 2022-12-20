#include "JobScheduler.h"

using namespace std;

void* task(void* vargp){
  unsigned k=0;
  for(int i=0; i<100000; i++)
    k+=i;
  cout << "task_completed\n";
  return 0;
}

int main(void){
  JobScheduler *jbo = new JobScheduler();
  jbo->initialize_scheduler(4);
  jbo->submit_job(new Job(task, NULL));
  jbo->submit_job(new Job(task, NULL));
  jbo->submit_job(new Job(task, NULL));
  jbo->wait_all_tasks_finish();
  jbo->wait_all_tasks_finish();
  //jbo->wait_all_tasks_finish();
  cout << "bye\n";
  cout << jbo->destroy_scheduler() << endl;
  exit(0);
}
