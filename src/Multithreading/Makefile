EXEC = mult
OBJS = main.o JobScheduler.o
CFLAGS = -std=c++11 -Wno-unused-parameter -g -w -pthread

$(EXEC): $(OBJS)
	g++ $(CFLAGS) $(OBJS) -o $(EXEC)

run:
	./$(EXEC)
clean:
	rm -rf ./$(EXEC) *.o
