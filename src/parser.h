#include <iostream>
#include <cstdlib>
#include <cstring>

using namespace std;

class Parser {
    char* relation[9];
public:
    Parser();
    ~Parser();
    void OpenFileAndParse(); //opens small.work and reads file line by line, extracting queries
    void ParseRelations(char* relations); // parses first part of query to find the relations of the query
    void ParsePredicates(char* predicates); // parses second part of query to find predicates of the query
    void ParseProjections(char* projections); // parses third part of query to find projections of query
};