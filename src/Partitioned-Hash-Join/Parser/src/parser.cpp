#include "parser.h"

/********************************* PARSER FUNCTIONS *********************************/
// CHANGE 1 TO 50!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
Parser::Parser() { queries = new Query*[1]; }

Parser::~Parser() {
    for (int i = 0 ; i < 1 ; i++) delete queries[i];
    delete [] queries;
}

Query* Parser::OpenFileAndParse() {
    // FILE *fp;
    // char *line = NULL; size_t len = 0; ssize_t read;
    // const char pipe_[2] = "|"; char *token; int counter; int q_no = 0; // query number

    // /* opening file for reading */
    // fp = fopen("src/small.work" , "r");
    // if(fp == NULL) { perror("Error opening file"); exit(-1);}
    // // each line that's not an F consist of 3 parts separated by pipes
    // // we store each one of these 3 parts onto an array.
    // char* parts[3];

    // while ((read = getline(&line, &len, fp)) != -1) {
    //     if(!strcmp(line, "F\n")) continue; // A batch of queries ended (we'll see what to do with this info)

    //     queries[q_no++] = new Query();
    //     counter=0;
    //     // finding each one of the 3 parts
    //     token = strtok(line, pipe_); /* get the first token */
    //     while( token != NULL ) { /* walk through other tokens */
    //         parts[counter++] = token;
    //         token = strtok(NULL, pipe_);
    //     }
    //     cout << REDFUL << "Parts: " << RESTORE << endl;
    //     for(int i=0; i<3; i++) cout << parts[i] << endl;
    //     // parsing each one of the 3 parts separately.
    //     // and making sure strtok pointer stays untouched by other functs
    //     if(queries[q_no-1]->ParseRelations(parts[0]) == IS_FINISHED) {
    //         if(queries[q_no-1]->ParsePredicates(parts[1]) == IS_FINISHED)
    //             queries[q_no-1]->ParseProjections(parts[2]);
    //     }
    // }
    // fclose(fp);
    // if (line) free(line); // doesnt work without free(even with delete, memory leaks)

    char line[50] = "0 1 2|0.2=1.0&0.1>2.1|1.2 0.1";
    char* parts[3];
    const char pipe_[2] = "|"; char *token; int counter; int q_no = 0; // query number
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

    return queries[q_no-1];
}

/********************************* QUERY FUNCTIONS *********************************/

Query::Query(): number_of_relations(0) {
    for(int i=0 ;i<5; i++) relation[i] = NULL;
    projections = new Projection*[3]; //each query has maximum of 3 columns to sum
    for (int i = 0 ; i < 3; i++) projections[i] = new Projection();

    prdcts = new Predicates*[4];// each query has maximum of 4 predicates
    for(int i=0; i<4; i++) prdcts[i] = new Predicates();
}

Query::~Query() {
    for (int i = 0 ; i < 4; i++) delete prdcts[i];
    delete [] prdcts;
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

    for(int i=0; i<5; i++)  {
        if(relation[i] != NULL) {
            number_of_relations++;
            cout << "relation[" << i << "]: " << relation[i] << endl;
        }
    }
    return IS_FINISHED;
}

int Query::ParsePredicates(char* predicates) {
    cout << "In predicates: " << predicates << endl;
    const char and_[2] = "&"; char *token; int index=0;

    token = strtok(predicates, and_);
    while( token != NULL ) { /* walk through other tokens */
        prdcts[index++]->setPredicates(token);
        token = strtok(NULL, and_);
    }
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

/********************************* PREDICATES FUNCTIONS *********************************/
Predicates::Predicates()
:number(0), relation_index_left(-1), relation_index_right(-1), column_left(-1), column_right(-1), relation_after_operation(false), number_after_operation(false), operation('x')
{memset(predicate, '\0', sizeof(predicate));}

Predicates::~Predicates() {}

void Predicates::setPredicates(char* prdct) {
    strcpy(predicate, prdct);
    // cout << "SEPARATED PREDICATES: ";
    // for(int i=0; i<15; i++) cout << predicate[i] ;
    cout << endl;
    // convert char to int
    relation_index_left = predicate[0] - '0';
    column_left = predicate[2] - '0';
    cout << "left relation: " << relation_index_left << " left column: " << column_left;

    // when small.work has > or < it's an operation with a number
    if(predicate[3] == '>' || predicate[3] == '<') {
        operation = predicate[3];
        number_after_operation = true;
        // since the number is stored one by one digit on the array we have to recreate it
        int decimal = 1;
        for(int i=14; i>3; i--) {
            if(predicate[i] != '\0') {
                number += (predicate[i] - '0') * decimal;
                decimal*=10;
            }
        }
        cout << " operation: " << operation << " number: " << number << endl;
    }else if(predicate[3] == '=') {
        operation = predicate[3];
        // if the 5th index is not '.' (dot character), it's a number not a relation
        if(predicate[5] != '.') {
            number_after_operation = true;
            // since the number is stored one by one digit on the array we have to recreate it
            int decimal = 1;
            for(int i=14; i>3; i--) {
                if(predicate[i] != '\0') {
                    number += (predicate[i] - '0') * decimal;
                    decimal*=10;
                }
            }
            cout << " operation: " << operation << " number: " << number << endl;
        }else if(predicate[5] == '.') {
            relation_after_operation = true;
            relation_index_right = predicate[4] - '0';
            column_right = predicate[6] - '0';
            cout << " operation: " << operation << " right relation: " << relation_index_right << " right column: " << column_right << endl;
        }
    }
}
