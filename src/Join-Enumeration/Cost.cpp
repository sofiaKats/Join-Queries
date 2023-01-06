#include "Cost.hpp"

using namespace std;

//MALLON THA PREPEI NA PAIRNEI SAN ORISMA TA RELATIONS
Cost::Cost(Query* q, Relation**){
    query = q;
    metadata = new tempMetadata**[q->number_of_relations];
}

//PRIORITY FOR IMPLEMENTATION, CASE 0, CASE 2
int Cost::cost(){
    int sum = 0;
    /*
    int cost;

    for (int i = 0; i < query->number_of_predicates; i++){
        switch (query->prdcts[i]->flag)
        {
        case 0:
            //0 will mean: Φίλτρα της μορφής σA=k (R.A = k)
            //cost = cost_FilterEquals();
            break;
        case 1:
            //1 will mean: Φίλτρα της μορφής σA=Β (R.A = R.Β)
            //cost = cost_FilterSelfJoin();
            break;
        case 2:
            //2 will mean: Ζεύξη μεταξύ δύο διαφορετικών πινάκων (R.A = S.B)
            //cost = cost_DiffRelations();
            break;
        case 3:
            //3 will mean: Αυτοσυσχέτιση (R.A = R.A) ?? MALLON AUTO ENNOEI 
            //cost = cost_SelfRelation();
            break;        
        default:
            break;
        }
        sum+=cost;
    }*/

    return sum;
}

Cost::Cost(){

}


