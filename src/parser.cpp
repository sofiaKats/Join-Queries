#include "parser.h"

/********************************* PARSER FUNCTIONS *********************************/

Parser::Parser() { queries = new Query*[50]; }

Parser::~Parser() { 
    for (int i = 0 ; i < 50 ; i++) delete queries[i];
    delete [] queries;
}

void Parser::OpenFileAndParse() {
    FILE *fp;
    char *line = NULL; size_t len = 0; ssize_t read;
    const char pipe_[2] = "|"; char *token; int counter; int q_no = 0; // query number

    /* opening file for reading */
    fp = fopen("src/small.work" , "r");
    if(fp == NULL) { perror("Error opening file"); exit(-1);}
    // each line that's not an F consist of 3 parts separated by pipes
    // we store each one of these 3 parts onto an array.
    char* parts[3];

    while ((read = getline(&line, &len, fp)) != -1) { 
        if(!strcmp(line, "F\n")) continue; // A batch of queries ended (we'll see what to do with this info)
        
        queries[q_no++] = new Query();
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
        queries[q_no-1]->ParseRelations(parts[0]);
        queries[q_no-1]->ParsePredicates(parts[1]);
        queries[q_no-1]->ParseProjections(parts[2]);

        
    }
    fclose(fp);
    if (line) free(line); // doesnt work without free(even with delete, memory leaks)
}

/********************************* QUERY FUNCTIONS *********************************/

Query::Query() {
    for(int i=0 ;i<9; i++) relation[i] = NULL;
}

Query::~Query() {}

void Query::ParseRelations(char* relations) {
    // cout << "RELATIONS: " << relations << endl;
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

void Query::ParsePredicates(char* predicates) {
    cout << "In predicates: " << predicates << endl;
}

void Query::ParseProjections(char* projections) {
    cout << "Projections: " << projections << endl; 
}