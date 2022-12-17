class Job{
public:
  Job(void* (*fn) (void*), void* args): routine(fn), arg(args){}
  void *(*routine) (void*);
	void *arg;
  Job* next = NULL;
};
