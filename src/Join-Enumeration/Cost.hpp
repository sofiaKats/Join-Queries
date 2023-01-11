#include "../Parser/parser.h"
#include "../Structures/Relation/Relation.hpp"

#include <iostream>
#include <cstdlib>
#include <cstring>

typedef struct tempMetadata{
  unsigned long int l;  // minimum value in column
  unsigned long int u;  // maximum value in column
  int f;                // number of values in column
  int d;                // number of distinct values in column
}tempMetadata;

class Cost{
private:
    Query* query;
    //tempMetadata*** metadata; 
    Metadata*** metadata;

    //Φίλτρα της μορφής σA=k (R.A = k)
    int cost_FilterEquals(); 

    //Φίλτρα της μορφής σA=Β (R.A = R.Β)
    int cost_FilterSelfJoin();

    //Ζεύξη μεταξύ δύο διαφορετικών πινάκων (R.A = S.B)
    int cost_DiffRelations();

    //Αυτοσυσχέτιση (R.A = R.A) ?? MALLON AUTO ENNOEI    
    int cost_SelfRelation();

    //Φίλτρα της μορφής σk1 <= A <= k2 ?? NOT TO BE IMPLEMENTED NOW
public:
    Cost();
    Cost(Relation**, Predicates*);

    int cost();        // (2.2 = 3.5 & 1.0 = 2.1)
};