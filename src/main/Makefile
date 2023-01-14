BLD = ./obj

# Compiler options
CC = g++
CFLAGS = -std=c++11 -Wno-unused-parameter -g -w -pthread

# Executable file names
EXE_PROG = ./program

# .o files needed
OBJS_PROG = main.o ../Multithreading/JobScheduler.o ../partition-phase/Partition.o ../partition-phase/PartitionedHashJoin.o ../hopscotch/src/Index.o ../hopscotch/src/Hashtable.o ../Structures/Relation/Relation.o ../Joiner/Joiner.o ../Parser/parser.o ../Join-Enumeration/BestTree.o ../Join-Enumeration/Cost.o ../Join-Enumeration/JoinEnum.o ../Join-Enumeration/JoinTreeList.o 

# Build executables
all: $(EXE_PROG)
#	mkdir -p $(BLD)
#	mv -f $(OBJS_PROG) $(BLD)

$(EXE_PROG): $(OBJS_PROG)
	$(CC) $(CFLAGS) $(OBJS_PROG) -o $(EXE_PROG)

# Delete executable & object files
clean:
	rm -f $(EXE_PROG)
	rm -rf $(OBJS_PROG)
	rm -rf $(BLD)

# Clean and compile
comp: clean all
