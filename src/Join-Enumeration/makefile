BLD = ./obj

# Compiler options
CC = g++
CFLAGS = -std=c++11 -Wno-unused-parameter -g -w -pthread

# Executable file names
EXE_PROG = ./enum

# .o files needed
OBJS_PROG = joinMain.o BestTree.o Cost.o JoinEnum.o JoinTreeList.o ../Parser/parser.o

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
