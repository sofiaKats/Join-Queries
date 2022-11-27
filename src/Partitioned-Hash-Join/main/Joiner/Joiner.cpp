#include "Joiner.hpp"

void Joiner::AddRelation(const char* fileName)
// Loads a relation from disk
{
  relations.emplace_back(fileName);
}
//---------------------------------------------------------------------------
Relation& Joiner::GetRelation(unsigned relationId)
// Loads a relation from disk
{
  if (relationId >= relations.size()) {
    char error[256];
    sprintf(error, "* relation with id: %d does not exist", relationId);
    throw runtime_error(error);
  }
  return relations[relationId];
}

RelColumn* Joiner::GetRelationCol(unsigned relationId, unsigned colId){
    uint64_t* column = GetRelation(relationId).columnsArr[colId];
    return NULL;
}

enum QueryGraphProvides {  Left, Right, Both, None };
//---------------------------------------------------------------------------
static QueryGraphProvides analyzeInputOfJoin(set<unsigned>& usedRelations,SelectInfo& leftInfo,SelectInfo& rightInfo)
  // Analyzes inputs of join
{
  bool usedLeft=usedRelations.count(leftInfo.binding);
  bool usedRight=usedRelations.count(rightInfo.binding);

  if (usedLeft^usedRight)
    return usedLeft?QueryGraphProvides::Left:QueryGraphProvides::Right;
  if (usedLeft&&usedRight)
    return QueryGraphProvides::Both;
  return QueryGraphProvides::None;
}
//---------------------------------------------------------------------------
string Joiner::Join(QueryInfo& query)
  // Executes a join query
{ //"3 0 1|0.2=1.0&0.1=2.0&0.2>3000|1.2 0.1";
  GetRelationCol(query.predicates[0].left.relId, query.predicates[0].left.colId);



  /*Checksum checkSum(move(root),query.selections);
  checkSum.run();

  stringstream out;
  auto& results=checkSum.checkSums;
  for (unsigned i=0;i<results.size();++i) {
    out << (checkSum.resultSize==0?"NULL":to_string(results[i]));
    if (i<results.size()-1)
      out << " ";
  }
  out << "\n";
  return out.str();*/
}
