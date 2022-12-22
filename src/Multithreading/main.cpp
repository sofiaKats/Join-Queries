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
  for(int i=0; i<8; i++)
    sch.submit_job(new Job(task, NULL));
  sch.wait_all_tasks_finish();
  sch.wait_all_tasks_finish();
  sch.wait_all_tasks_finish();
  cout << "bye\n";
  cout << sch.destroy_scheduler() << endl;
  exit(0);
}
