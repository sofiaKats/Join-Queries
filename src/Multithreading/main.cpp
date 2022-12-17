#include "JobScheduler.h"

using namespace std;

int main(void){
  JobScheduler *jbo = new JobScheduler();
  jbo->initialize_scheduler(4);
  //jbo->wait_all_tasks_finish();
  cout << "bye\n";
  cout << jbo->destroy_scheduler() << endl;
  exit(0);
}
