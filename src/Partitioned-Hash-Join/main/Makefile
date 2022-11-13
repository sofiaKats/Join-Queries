EXEC = program
OBJS = Main.o ../partition-phase/Partition.o ../partition-phase/PartitionedHashJoin.o Parser.o ../hopscotch/src/Index.o ../hopscotch/src/Hashtable.o
CFLAGS = -std=c++11 -Wno-unused-parameter -g -w

$(EXEC): $(OBJS)
	g++ $(CFLAGS) $(OBJS) -o $(EXEC)

run:
	./$(EXEC)
clean:
	rm -rf ./$(EXEC) *.o ../partition-phase/*.o ../hopscotch/src/*.o
