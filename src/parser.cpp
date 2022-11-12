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
        while( token != NULL ) { /* walk through other tokens */
            parts[counter++] = token;
            token = strtok(NULL, pipe_);
        }
        cout << REDFUL << "Parts: " << RESTORE << endl;
        for(int i=0; i<3; i++) cout << parts[i] << endl;
        // parsing each one of the 3 parts separately.
        // and making sure strtok pointer stays untouched by other functs
        if(queries[q_no-1]->ParseRelations(parts[0]) == IS_FINISHED) {
            if(queries[q_no-1]->ParsePredicates(parts[1]) == IS_FINISHED)
                queries[q_no-1]->ParseProjections(parts[2]);
        }
    }
    fclose(fp);
    if (line) free(line); // doesnt work without free(even with delete, memory leaks)
}

/********************************* QUERY FUNCTIONS *********************************/

Query::Query() {
    for(int i=0 ;i<9; i++) relation[i] = NULL;
    projections = new Projection*[3]; //each query has maximum of 3 columns to sum
    for (int i = 0 ; i < 3; i++) projections[i] = new Projection();
}

Query::~Query() {
    for (int i = 0 ; i < 3; i++) delete projections[i];
    delete [] projections;
}

int Query::ParseRelations(char* relations) {
    // cout << "RELATIONS: " << relations << endl;
    const char space[2] = " "; char *token; int index = 0;

    token = strtok(relations, space);
    while( token != NULL ) { /* walk through other tokens */
        relation[index++] = token;
        token = strtok(NULL, space);
    }

    for(int i=0; i<9; i++)  {
        if(relation[i] != NULL) {
            cout << "relation[" << i << "]: " << relation[i] << endl;
        }
    }
    return IS_FINISHED;
}

int Query::ParsePredicates(char* predicates) {
    cout << "In predicates: " << predicates << endl;
    return IS_FINISHED;
}

void Query::ParseProjections(char* projection) {
    cout << "Projections: " << projection << endl; 
    const char space[2] = " "; char *token; int index=0; 

    token = strtok(projection, space);
    while( token != NULL ) { 
        projections[index++]->setRelation_Column_Pair(token);
        projections[index-1]->separateRelationFromColumn(); // store relation and column to be summed
        token = strtok(NULL, space); // find next relation-column pair
    }
}

/********************************* PROJECTION FUNCTIONS *********************************/
Projection::Projection(int relation, int column)
: relation_index(relation), column(column) { memset(relation_column_pair, '\0', sizeof(relation_column_pair)); }

Projection::~Projection() {}

void Projection::setRelationIndex(const int index) { relation_index = index; }

int Projection::getRelationIndex(void) { return relation_index; }

void Projection::setColumn(const int col) { column = col; }

int Projection::getColumn(void) { return column;}

void Projection::setRelation_Column_Pair(char* relation_column) { strcpy(relation_column_pair, relation_column); }

void Projection::separateRelationFromColumn(void){
    // convert char to int
    relation_index = relation_column_pair[0] - '0';
    column = relation_column_pair[2] - '0';

    cout << "RELATION: " << relation_index << " AND COLUMN: " << column << endl;
}

