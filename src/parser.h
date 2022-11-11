#include <iostream>
#include <cstdlib>
#include <cstring>

using namespace std;

class Query {
    char* relation[9];
public:
    Query();
    ~Query();
    void ParseRelations(char* relations); // parses first part of query to find the relations of the query
    void ParsePredicates(char* predicates); // parses second part of query to find predicates of the query
    void ParseProjections(char* projections); // parses third part of query to find projections of query
};

class Parser {
    Query** queries;
public:
    Parser();
    ~Parser();
    void OpenFileAndParse(); //opens small.work and reads the file line by line, extracting queries
};