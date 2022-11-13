#include "Parser.h"

Column* Parser::ParseRelation(string fileName){
  ifstream file;
  string line;
  string val;
  Column * rel;
  int i = 0, numLines = 0;

  file.open(fileName);

  if (!file.is_open()){
    cout << "Could not open file: " << fileName << endl;
    return NULL;
  }

  while (getline(file, line)){ //count lines to allocate relation table
    if (line.empty()) break;
    ++numLines;
  }
  file.clear();
  file.seekg(0);

  rel = new Column(numLines);

  while(getline(file, line)){
    if (line.empty()) break;
    stringstream input_stringstream(line);
    getline(input_stringstream, val, '|');
    rel->tuples[i].key = i;
    rel->tuples[i++].payload = stol(val);
  }

  file.close();

  return rel;
}

Column** Parser::ReadRelations(char* in_dir, int& length){
  DIR* FD;
  struct dirent* in_file;
  FILE* entry_file;
  int i = 0, numFiles = 0;

  if (NULL == (FD = opendir (in_dir)))
  {
        fprintf(stderr, "Error : Failed to open input directory - %s\n", strerror(errno));
        exit(EXIT_FAILURE);
  }
  while ((in_file = readdir(FD)))
  {
        if (!strcmp (in_file->d_name, "."))
            continue;
        if (!strcmp (in_file->d_name, ".."))
            continue;
        numFiles++;
  }
  rewinddir(FD);

  Column** relArray = new Column*[numFiles];

  while ((in_file = readdir(FD)))
  {
        if (!strcmp (in_file->d_name, "."))
            continue;
        if (!strcmp (in_file->d_name, ".."))
            continue;
        char filePath[500];
        sprintf(filePath, "%s/%s", in_dir, in_file->d_name);
        relArray[i++] = ParseRelation(filePath);
  }
  closedir(FD);
  return relArray;
}

int** Parser::ReadQueries(int& numLines){
    FILE * fp;
    char * line = NULL;
    size_t len = 0;
    ssize_t read;
    char *token;
    const char s[2] = " ";

    fp = fopen("Queries/Queries.tbl", "r");
    if (fp == NULL){
        cout << "Could not open file" << endl;
        exit(EXIT_FAILURE);
    }

    while ((read = getline(&line, &len, fp)) != -1) { //count lines to allocate relation table
        if (read == 0) break;
        ++numLines;
    }

    int** relIdsToJoin = new int*[numLines];
    numLines = 0;
    int i;
    rewind(fp);
    while ((read = getline(&line, &len, fp)) != -1) {
        i = 0;

        relIdsToJoin[numLines] = new int[2];
        /* get the first token */
        token = strtok(line, s);

        /* walk through other tokens */
        while( token != NULL) {
            int ia = atoi(token);
            relIdsToJoin[numLines][i++] = ia;

            token = strtok(NULL, s);

        }
        numLines++;
    }

    fclose(fp);

    return relIdsToJoin;
}
