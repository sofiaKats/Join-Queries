#include "parser.h"

/********************************* PARSER FUNCTIONS *********************************/
// CHANGE 1 TO 50!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
Parser::Parser() {
    q = new Queries();
    r = new Rels();
}

Parser::~Parser() {
    delete q;
    delete r;
}

Queries* Parser::OpenQueryFileAndParse() {
    FILE *fp;
    char *line = NULL; size_t len = 0; ssize_t read;
    const char pipe_[2] = "|"; char *token; int counter; int q_no = 0; // query number
    unsigned lines = 0;

    /* opening /file for reading */
    fp = fopen("./workloads/small/small.work" , "r");
    //fp = fopen("../main/workloads/small/small.work" , "r");

    if(fp == NULL) { perror("Error opening file"); exit(-1);}



    while ((read = getline(&line, &len, fp)) != -1) {
        if (read == 0) break;
        lines++;
    }
    rewind(fp);

    q->queries_arr = new Query*[lines];
    q->size = lines;
    //We now store each query in the array, which we return to main

    // each line that's not an F consist of 3 parts separated by pipes
    // we store each one of these 3 parts onto an array.
    char* parts[3];

    while ((read = getline(&line, &len, fp)) != -1) {
        if (read == 0) break;
        if(!strcmp(line, "F\n")) {
            //cout << BLUE << "END OF BATCH! " << RESTORE << endl;
            q->queries_arr[q_no++] = NULL;
            continue; // A batch of queries ended
        }

        q->queries_arr[q_no++] = new Query();
        counter=0;
        // finding each one of the 3 parts
        token = strtok(line, pipe_); /* get the first token */
        while( token != NULL ) { /* walk through other tokens */
            parts[counter++] = token;
            token = strtok(NULL, pipe_);
        }
        //cout << REDFUL << "Parts: " << RESTORE << endl;
        //for(int i=0; i < 3; i++) cout << parts[i] << endl;

        // parsing each one of the 3 parts separately.
        // and making sure strtok pointer stays untouched by other functs
        if(q->queries_arr[q_no-1]->ParseRelations(parts[0]) == IS_FINISHED) {
            if(q->queries_arr[q_no-1]->ParsePredicates(parts[1]) == IS_FINISHED)
                q->queries_arr[q_no-1]->ParseProjections(parts[2]);
        }
        q->queries_arr[q_no-1]->ReplacePredicateIndexWithRelation();
        q->queries_arr[q_no-1]->PredicatePriority();
    }
    fclose(fp);
    if (line) free(line); // doesnt work without free(even with delete, memory leaks)
    return q;
}

/********************************* QUERY FUNCTIONS *********************************/

Query::Query(): number_of_relations(0), number_of_projections(0), number_of_predicates(0){
    for(int i=0 ;i<15; i++) relation[i] = NULL;
    for(int i=0; i<15; i++) priority_predicates[i] = -1;
    projections = new Projection*[15]; //each query has maximum of 3 columns to sum
    for (int i = 0 ; i < 15; i++) projections[i] = new Projection();

    prdcts = new Predicates*[15];// each query has maximum of 4 predicates
    for(int i=0; i<15; i++) prdcts[i] = new Predicates();

    // priority_predicates = new Predicates*[15];
    // for(int i=0; i<15; i++) priority_predicates[i] = new Predicates();
}

Query::~Query() {
    // for (int i = 0 ; i < 15; i++) delete priority_predicates[i];
    // delete [] priority_predicates;
    for (int i = 0 ; i < 15; i++) delete prdcts[i];
    delete [] prdcts;
    for (int i = 0 ; i < 15; i++) delete projections[i];
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

    for(int i=0; i<15; i++)  {
        if(relation[i] != NULL) {
            number_of_relations++;
            //cout << "relation[" << i << "]: " << relation[i] << endl;
        }
    }
    return IS_FINISHED;
}

int Query::ParsePredicates(char* predicates) {
    //cout << "In predicates: " << predicates << endl;
    const char and_[2] = "&"; char *token; int index=0;

    token = strtok(predicates, and_);
    while( token != NULL ) { /* walk through other tokens */
        prdcts[index++]->setPredicates(token);
        number_of_predicates++;
        token = strtok(NULL, and_);
    }
    //cout << "number of predicates: " << number_of_predicates << endl;
    return IS_FINISHED;
}

void Query::ParseProjections(char* projection) {
    //cout << "Projections: " << projection << endl;
    const char space[2] = " "; char *token; int index=0;

    token = strtok(projection, space);
    while( token != NULL ) {
        number_of_projections++;
        projections[index++]->setRelation_Column_Pair(token);
        projections[index-1]->separateRelationFromColumn(); // store relation and column to be summed
        token = strtok(NULL, space); // find next relation-column pair
    }
    //cout << "number of projections: "<< number_of_projections << endl;
}

void Query::ReplacePredicateIndexWithRelation(void) {
    for(int i=0; i<number_of_predicates; i++) {
        prdcts[i]->relation_left = atoi(relation[prdcts[i]->binding_left]);
        if(prdcts[i]->relation_after_operation == true) prdcts[i]->relation_right = atoi(relation[prdcts[i]->binding_right]);
    }

    // set binding and relation for every projection
    for(int i=0; i<number_of_projections; i++) {
        projections[i]->setRealRelation(atoi(relation[projections[i]->getRelationIndex()]));
    }
}

void Query::PredicatePriority(void) {
    int priority_index = 0;
    // filters first
    for(int i=0; i<number_of_predicates; i++) {
        if(prdcts[i]->number_after_operation == true) {
            //store index of prdct array in priority array
            priority_predicates[priority_index++] = i;
            prdcts[i]->filter = true; // operation is a filter

            //NEW
            if (prdcts[i]->operation == '=') prdcts[i]->flag = FILTER_EQUALS;
            else prdcts[i]->flag = FILTER_BIGGER_SMALLER;
        }
    }

    //then self joins
    for(int i=0; i<number_of_predicates; i++) {
        if(prdcts[i]->relation_after_operation == true) {
            //if its a self join, the priority is higher
            if(prdcts[i]->binding_left == prdcts[i]->binding_right) {
                priority_predicates[priority_index++] = i;
                prdcts[i]->self_join = true;

                //NEW
                if (prdcts[i]->column_left == prdcts[i]->column_right)
                    prdcts[i]->flag = SELF_RELATION;
                else 
                    prdcts[i]->flag = FILTER_SELF_JOIN;
            }
        }
    }
    // then the rest of the joins
    //add the rest of the joins
    for(int i=0; i<number_of_predicates; i++) {
        if(prdcts[i]->self_join == true || prdcts[i]->filter==true) continue;
        if(prdcts[i]->relation_after_operation == true && prdcts[i]->binding_left != prdcts[i]->binding_right) {
            priority_predicates[priority_index++] = i; //assign first predicate we find
            prdcts[i]->simple_join = true;

            //NEW
            prdcts[i]->flag = DIFF_RELATIONS;
        }
    }

    /*for(int i=0; i<number_of_predicates; i++)
        cout << "predicate priority " << i << ": " << prdcts[priority_predicates[i]]->predicate << " binding left: " << prdcts[priority_predicates[i]]->binding_left << " binding right: "  << prdcts[priority_predicates[i]]->binding_right << " relation left: " << prdcts[priority_predicates[i]]->relation_left << " relation right: " << prdcts[priority_predicates[i]]->relation_right
        << " filter?: " << prdcts[priority_predicates[i]]->filter << " self-join?: " << prdcts[priority_predicates[i]]->self_join << " simple-join?: " << prdcts[priority_predicates[i]]->simple_join << endl;

    for(int i=0; i<number_of_projections; i++)
        cout << "projection " << i << ": real relation: " << projections[i]->getRealRelation() << " binding index: " << projections[i]->getRelationIndex() << " column: " << projections[i]->getColumn() << endl;
    */
}


/********************************* PROJECTION FUNCTIONS *********************************/
Projection::Projection(int relation, int column)
: relation_index(relation), column(column), real_relation(-1) { memset(relation_column_pair, '\0', sizeof(relation_column_pair)); }

Projection::~Projection() {}

void Projection::setRelationIndex(const int index) { relation_index = index; }

int Projection::getRelationIndex(void) { return relation_index; }

void Projection::setColumn(const int col) { column = col; }

int Projection::getColumn(void) { return column;}

int Projection::getRealRelation(void) { return real_relation; }

void Projection::setRealRelation(int relation) { real_relation= relation; }

void Projection::setRelation_Column_Pair(char* relation_column) { strcpy(relation_column_pair, relation_column); }

void Projection::separateRelationFromColumn(void){
    // convert char to int
    relation_index = relation_column_pair[0] - '0';
    column = relation_column_pair[2] - '0';

    //cout << "RELATION: " << relation_index << " AND COLUMN: " << column << endl;
}

/********************************* PREDICATES FUNCTIONS *********************************/
Predicates::Predicates()
:number(0), binding_left(-1), binding_right(-1), column_left(-1), column_right(-1), relation_after_operation(false), number_after_operation(false), operation('x'), filter(false), self_join(false), simple_join(false), relation_left(-1), relation_right(-1)
{memset(predicate, '\0', sizeof(predicate));}

Predicates::~Predicates() {}

void Predicates::setPredicates(char* prdct) {
    strcpy(predicate, prdct);
    // cout << "SEPARATED PREDICATES: ";
    // for(int i=0; i<15; i++) cout << predicate[i] ;
    //cout << endl;
    // convert char to int
    binding_left = predicate[0] - '0';
    column_left = predicate[2] - '0';
    //cout << "left relation: " << binding_left << " left column: " << column_left;

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
        //cout << " operation: " << operation << " number: " << number << endl;
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
            //cout << " operation: " << operation << " number: " << number << endl;
        // if the 5th index is '.' (dot character), it's a number not a relation
        }else if(predicate[5] == '.') {
            relation_after_operation = true;
            binding_right = predicate[4] - '0';
            column_right = predicate[6] - '0';
            //cout << " operation: " << operation << " right relation: " << binding_right << " right column: " << column_right << endl;
        }
    }
}


Rels* Parser::OpenRelFileAndParse(){
    FILE *fp;
    char *line = NULL;
    char path[50];
    size_t len = 0;
    ssize_t read;
    unsigned filesCount = 0;
    char** paths;

    fp = fopen("./workloads/small/small.init", "r");
    //fp = fopen("../main/workloads/small/small.init", "r");
    if (fp == NULL){
        cout << "Could not open init file" << endl;
        exit(EXIT_FAILURE);
    }
    while ((read = getline(&line, &len, fp)) != -1) { //count lines to allocate relation table
        if (read == 0) break;
        ++filesCount;
    }
    r->paths = new char*[filesCount];
    r->size = filesCount;

    filesCount = 0;
    rewind(fp);
    while ((read = getline(&line, &len, fp)) != -1) { //count lines to allocate relation table
        if (read == 0) break;
        line[strlen(line)-1] = '\0';
        r->paths[filesCount] = new char[50];
        //sprintf(r->paths[filesCount++], "./workloads/small/%s", line);
        sprintf(r->paths[filesCount++], "../main/workloads/small/%s", line);

    }
    return r;
}
