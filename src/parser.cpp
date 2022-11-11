#include "parser.h"

Parser::Parser() { 
    for(int i=0 ;i<9; i++) relation[i] = NULL;
}

Parser::~Parser() {}

void Parser::OpenFileAndParse() {
    FILE *fp;
    char *line = NULL; size_t len = 0; ssize_t read;
    const char pipe_[2] = "|"; char *token; int counter;

    /* opening file for reading */
    fp = fopen("src/small.work" , "r");
    if(fp == NULL) { perror("Error opening file"); exit(-1);}
    // each line that's not an F consist of 3 parts separated by pipes
    // we store each one of these 3 parts onto an array.
    char* parts[3];

    while ((read = getline(&line, &len, fp)) != -1) { 
        counter=0;
        // finding each one of the 3 parts
        token = strtok(line, pipe_); /* get the first token */
        parts[counter] = token;

        while( token != NULL ) { /* walk through other tokens */
            parts[counter++] = token;
            token = strtok(NULL, pipe_);
        }
        cout << "Parts: " << endl;
        for(int i=0; i<3; i++) cout << parts[i] << endl;
        // parsing each one of the 3 parts separately.
        ParseRelations(parts[0]);
        ParsePredicates(parts[1]);
        ParseProjections(parts[2]);

        
    }
    fclose(fp);
    if (line) free(line); // doesnt work without free(even with delete, memory leaks)
}

void Parser::ParseRelations(char* relations) {
    const char space[2] = " "; char *token; int index = 0;

    token = strtok(relations, space);
    relation[index] = token;

    while( token != NULL ) { /* walk through other tokens */
        relation[index++] = token;
        token = strtok(NULL, space);
    }

    for(int i=0; i<9; i++)  {
        if(relation[i] != NULL) {
            cout << "relation[" << i << "]: " << relation[i] << endl;
        }
    }
}

void Parser::ParsePredicates(char* predicates) {
    cout << "In predicates: " << predicates << endl;
}

void Parser::ParseProjections(char* projections) {
    cout << "Projections: " << projections << endl; 
}