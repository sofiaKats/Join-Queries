EXEC = program
OBJS = main.o ../partition-phase/Partition.o ../partition-phase/PartitionedHashJoin.o ../hopscotch/src/Index.o ../hopscotch/src/Hashtable.o ../Relation/Relation.cpp Parsing/Operators.o Parsing/Parser.o Parsing/Joiner.o
CFLAGS = -std=c++11 -Wno-unused-parameter -g -w

$(EXEC): $(OBJS)
	g++ $(CFLAGS) $(OBJS) -o $(EXEC)

run:
	./$(EXEC)
clean:
	rm -rf ./$(EXEC) *.o ../partition-phase/*.o ../hopscotch/src/*.o ./Parsing/*.o
