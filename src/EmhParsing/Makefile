SRC = ./src

# .o files will be stored here
BLD = ./obj

# Compiler options
CC = g++
CFLAGS = -std=c++11 -Wno-unused-parameter -g -w

# Executable file names
EXE_PARSER = ./parser

# .o files needed
OBJS_PARSER =  $(SRC)/parser.o $(SRC)/main.o

# Build executables
all: $(EXE_PARSER)
	mkdir -p $(BLD)
	mv -f $(OBJS_PARSER) $(BLD)

$(EXE_PARSER): $(OBJS_PARSER)
	$(CC) $(CFLAGS) $(OBJS_PARSER) -o $(EXE_PARSER)

# Delete executable & object files
clean:
	rm -f $(EXE_PARSER)
	rm -rf $(BLD)

# Clean and compile
comp: clean all