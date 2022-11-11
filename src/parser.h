#include <iostream>
#include <cstdlib>
#include <cstring>

using namespace std;

class Parser {
    char* relation[9];
public:
    Parser();
    ~Parser();
    void OpenFileAndParse();
    void ParseRelations(char* relations);
    void ParsePredicates(char* predicates);
    void ParseProjections(char* projections);
};