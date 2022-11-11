#include <iostream>
#include <cstdlib>
#include <cstring>

using namespace std;

class Parser {

public:
    Parser();
    ~Parser();
    void OpenFileAndParse();
    void ParseRelations(char* relations);
    void ParsePredicates(char* predicates);
    void ParseProjections(char* projections);
};