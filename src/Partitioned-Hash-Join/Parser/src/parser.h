#include <iostream>
#include <cstdlib>
#include <cstring>

#define IS_FINISHED 1
#define REDFUL "\033[3;101;37m"
#define RESTORE "\033[0m" 

using namespace std;

// small.work file has 50 queries. These queries can have up to 4 relations,
// up to 4 predicates and up to 3 projections

class Predicates {
public:
    char predicate[15]; // store each predicate to later check if it is filter predicate or join predicate
    int relation_index_left;// represents the relation BEFORE the operation of the predicate
    int column_left;        // represents the column BEFORE the operation of the predicate
    char operation;      // represents the operation to be done in the particular predicate
    bool relation_after_operation; // flag to check if there is a relation after the operation of the predicate
    // can be used to speed up the filter process of the relations
    bool number_after_operation; // flag to check if there is a number after the operation of the predicate
    int number;      // the number filter after the operation if it exists    
    int relation_index_right; // the relation after the operation if it exists
    int column_right;  // the column after the operation if it exists
    // flags indicating the nature of a predicate
    bool filter;
    bool self_join;
    bool simple_join;
    Predicates();
    ~Predicates();
    void setPredicates(char* prdct);
};

class Projection {
private:
    // holds the index of the relation we are going to use for the SUM
    int relation_index; // (ex. projections: 0.2 1.3, relation_index will hold 0 and 1 in two separate objects)
    int column;         // holds the column of the relation we use for the SUM
    char relation_column_pair[5]; // used to separate the relation and the column from the dot .
public:
    Projection(int relation = -1, int column = -1);
    ~Projection();
    void setRelationIndex(const int index);
    int getRelationIndex(void);
    void setColumn(const int col);
    int getColumn(void);
    void setRelation_Column_Pair(char* relation_column);
    void separateRelationFromColumn(void);
};

class Query {
public:
    char* relation[15]; // the relations that are to be used in the particular query
    int number_of_relations;
    Predicates** prdcts;
    Projection** projections; // each object holds the relation and column to be SUMMED from part 3 of query 
    int number_of_projections;
    int number_of_predicates;
    int priority_predicates[15];// index 0:highest priority, index 15: lowest priority, each index holds the index of the
    // predicate from the prdcts array (assuming that the only filter of all the predicates is stored in index:5 of prdcts array
    // it will be priority_predicates[0] = 5)
    Query();
    ~Query();
    int ParseRelations(char* relations); // parses first part of query to find the relations of the query
    int ParsePredicates(char* predicates); // parses second part of query to find predicates of the query
    void ParseProjections(char* projection); // parses third part of query to find projections of query
    void PredicatePriority(void); // out of the predicates this function finds what to do first(1. filters, 2. self join, 3. other joins)
};

class Parser {
    Query** queries;
public:
    Parser();
    ~Parser();
    Query* OpenFileAndParse(); //opens small.work and reads the file line by line, extracting queries
};