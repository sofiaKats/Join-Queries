#include <iostream>
#include <cstdlib>

#include "parser.h"

using namespace std;

// execute with: ./parser
int main(void) {
    Parser parser;
    parser.OpenFileAndParse();
    return 0;
}