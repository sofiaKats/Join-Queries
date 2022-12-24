#include "Cost.hpp"

using namespace std;

//MALLON THA PREPEI NA PAIRNEI SAN ORISMA TA RELATIONS
Cost::Cost(Query* q, Relation**){
    query = q;
    metadata = new tempMetadata**[q->number_of_relations];
}

//PRIORITY FOR IMPLEMENTATION, CASE 0, CASE 2
int Cost::cost(char* predicates){
    for (int i = 0; i < query->number_of_predicates; i++){
        switch (query->prdcts[i]->flag)
        {
        case 0:
            //0 will mean: Φίλτρα της μορφής σA=k (R.A = k)
            cost_FilterEquals();
            break;
        case 1:
            //1 will mean: Φίλτρα της μορφής σA=Β (R.A = R.Β)
            cost_FilterSelfJoin();
            break;
        case 2:
            //2 will mean: Ζεύξη μεταξύ δύο διαφορετικών πινάκων (R.A = S.B)
            cost_DiffRelations();
            break;
        case 3:
            //3 will mean: Αυτοσυσχέτιση (R.A = R.A) ?? MALLON AUTO ENNOEI 
            cost_SelfRelation();
            break;
        
        default:
            break;
        }
    }
}



