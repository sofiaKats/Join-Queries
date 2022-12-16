#include "JobScheduler.h"

using namespace std;

int main(void){
  JobScheduler *jbo = new JobScheduler();
  jbo->initialize_scheduler(4);
  jbo->wait_all_tasks_finish(jbo);
  cout << "bye\n";
  cout << jbo->destroy_scheduler(jbo);
  exit(0);
}
