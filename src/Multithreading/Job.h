class Job{
public:
  Job(void*, void*);
  void (*routine) (void*);
	void * arg;
  Job* next = NULL;
};
