#include "Cost.hpp"
#include <math.h>
#include <stdio.h>

using namespace std;

//  Cost Constructor for just one predicate
Cost::Cost(Relation** rels, Predicates* p, int relSize){
    metadata = new Metadata**[relSize];
    columns = new int[relSize];
    this->size = relSize;
    for (int i = 0; i < relSize; i++) {
        metadata[i] = rels[i]->column_metadata;
        columns[i] = rels[i]->numColumns;
    }
    this->p = p;
    cost = 0;
}

//---------------------------------------------------------------------------

Cost::Cost(Predicates* p, Cost* c){
    metadata = new Metadata**[c->size];
    columns = new int[c->size];
    this->size = c->size;
    for (int i = 0; i < size; i++) {
        metadata[i] = c->metadata[i];
        columns[i] = c->columns[i];
    }
    this->p = p;
    this->cost = c->cost;
}

//---------------------------------------------------------------------------

int Cost::findCost(){
    int newCost = 0;
    switch (p->flag)
    {
    case 0:
        //0 will mean: Φίλτρα της μορφής σA=k (R.A = k)
        newCost = cost_FilterEquals();
        break;
    case 1:
        //1 will mean: Φίλτρα της μορφής σk1 <= A <= k2
        newCost = cost_FilterSmallerBigger();
        break;
    case 2:
        //2 will mean: Φίλτρα της μορφής σA=Β (R.A = R.Β)
        newCost = cost_FilterSelfJoin();
        break;
    case 3:
        //3 will mean: Ζεύξη μεταξύ δύο διαφορετικών πινάκων (R.A = S.B)
        newCost = cost_DiffRelations();
        break;
    case 4:
        //4 will mean: Αυτοσυσχέτιση (R.A = R.A) ?? MALLON AUTO ENNOEI 
        newCost = cost_SelfRelation();
        break;        
    default:
        break;
    }
    cost+=newCost;
    return cost;
}
//---------------------------------------------------------------------------

int Cost::cost_FilterEquals(){
//..............for the specific column.........................
    int col = p->column_left;
    int k = p->number;

    //set new values
    metadata[p->relation_left][col]->setL(k);
    metadata[p->relation_left][col]->setU(k);

    int F_a = metadata[p->relation_left][col]->getF();
    int D_a = metadata[p->relation_left][col]->getD();

    if (!metadata[p->relation_left][col]->checkXdistinct(k)){
        if (D_a != 0) metadata[p->relation_left][col]->setF(F_a / D_a);
        metadata[p->relation_left][col]->setD(1);
    }
    else {
        metadata[p->relation_left][col]->setD(0);
        metadata[p->relation_left][col]->setF(0);
    }
//..............for the other columns...............................
    for (int i = 0; i < columns[p->relation_left]; i++){
        if (i == col) continue; 

        int F_c = metadata[p->relation_left][i]->getF();
        int D_c = metadata[p->relation_left][i]->getD();

        //  set new values
        //  u, l dont change
        if (D_c != 0 && F_a != 0) metadata[p->relation_left][i]->setD(D_c * (1 - pow(1 - metadata[p->relation_left][col]->getF() / F_a, F_c / D_c)) );
        metadata[p->relation_left][i]->setF(metadata[p->relation_left][col]->getF());
    }
    return metadata[p->relation_left][col]->getF();
}

//---------------------------------------------------------------------------


//done? I think
int Cost::cost_FilterSmallerBigger(){
//..............for the specific column.........................
    int rel = p->relation_left;
    int col = p->column_left;
    int k1 = p->number;
    int k2 = p->number;

    int U = metadata[rel][col]->getU();
    int D = metadata[rel][col]->getD();
    int L = metadata[rel][col]->getL();
    int F = metadata[rel][col]->getF();

    //make sure k is within limits
    if (p->operation == '>'){
        if (k1 < L) { k1 = L; }
        k2 = U; 
    }
    else if (p->operation == '<'){
        if (k2 > U){ k2 = U; }
        k1 = L;
    }

    // set new values
    metadata[rel][col]->setL(k1);
    metadata[rel][col]->setU(k2);

    int F_new = F;
    if (U != L){
        int D_new = ((k2 - k1) / (U - L)) * D;
        int F_new = ((k2 - k1) / (U - L)) * F;
        metadata[rel][col]->setD(D_new);
        metadata[rel][col]->setF(F_new);
    }

//..............for the other columns............................
    for (int i = 0; i < columns[rel]; i++){
        if (i == col) continue;        
        int D_c = metadata[rel][i]->getD();
        int F_c= metadata[rel][i]->getF();

        // set new values
        // l, u stay the same
        if (D_c != 0 && F != 0) metadata[rel][i]->setD(D_c * (1 - pow(1 - F_new / F, F_c / D_c)));
        metadata[rel][i]->setF(F_new);
    }
    return F_new;    
}

//---------------------------------------------------------------------------


//done? I think
int Cost::cost_SelfRelation(){
//..............for the specific column.........................
    int rel = p->relation_left;
    int col = p->column_left;

    int L = metadata[rel][col]->getL();
    int U = metadata[rel][col]->getU(); 
    int D = metadata[rel][col]->getD();
    int F = metadata[rel][col]->getF();
    int n = U - L + 1;

    // set new values
    // l, u, d stay the same
    int F_new = F * F / n;
    metadata[rel][col]->setF(F_new);

//..............for the other columns............................
    for (int i = 0; i < columns[rel]; i++){
        if (i == col) continue;

        // set new values
        // l, u, d stay the same
        metadata[rel][i]->setF( F_new);
    }
    return F_new;
}

//---------------------------------------------------------------------------


//done?
int Cost::cost_FilterSelfJoin(){
//..............for the specific columns.........................
    int rel = p->relation_left;
    int colA = p->column_left;
    int colB = p->column_right;

    int L_a = metadata[rel][colA]->getL();
    int U_a = metadata[rel][colA]->getU(); 
    int D_a = metadata[rel][colA]->getD();
    int F_a = metadata[rel][colA]->getF();

    int L_b = metadata[rel][colB]->getL();
    int U_b = metadata[rel][colB]->getU();

    // set new values
    metadata[rel][colA]->setL(max(L_a, L_b));
    metadata[rel][colB]->setL(max(L_a, L_b));

    metadata[rel][colA]->setU(min(U_a, U_b));
    metadata[rel][colB]->setU(min(U_a, U_b));

    int n = metadata[rel][colA]->getU() - metadata[rel][colA]->getL() + 1;

    int F_new = F_a / n;
    metadata[rel][colA]->setF(F_new);
    metadata[rel][colB]->setF(F_new);

   
    if (D_a != 0 && F_a != 0 ) metadata[rel][colA]->setD(D_a * (1 - pow(1 - F_new / F_a, F_a / D_a)));
    if (D_a != 0 && F_a != 0) metadata[rel][colB]->setD(D_a * (1 - pow(1 - F_new / F_a, F_a / D_a)));

//..............for the other columns............................
    for (int i = 0; i < columns[rel]; i++){
        if (i == colA || i == colB) continue;

        int D_c = metadata[rel][i]->getD();
        
        // set new values
        // l, u stay the same
        if (D_c != 0 && F_a != 0) metadata[rel][i]->setD(D_c * (1 - pow(1 - F_new / F_a, F_a / D_c)));
        metadata[rel][i]->setF(F_new);
    }
    return F_new;
}
//---------------------------------------------------------------------------

//done?
int Cost::cost_DiffRelations(){
//..............for the specific columns.........................
    int relA = p->relation_left;
    int relB = p->relation_right;

    int colA = p->column_left;
    int colB = p->column_right;

    int L_a = metadata[relA][colA]->getL();
    int U_a = metadata[relA][colA]->getU(); 
    int D_a = metadata[relA][colA]->getD();
    int F_a = metadata[relA][colA]->getF();

    int L_b = metadata[relB][colB]->getL();
    int U_b = metadata[relB][colB]->getU();
    int F_b = metadata[relB][colB]->getF();
    int D_b = metadata[relB][colB]->getD();

    //cout << "Da Db are " << D_a << " " << D_b << endl;

    //make sure new L, U are within limits
    int L_new = max(L_a, L_b);
    int U_new = min(U_a, U_b);

    int n = U_new - L_new + 1;

    // set new values
    metadata[relA][colA]->setU(U_new);
    metadata[relB][colB]->setU(U_new);

    metadata[relA][colA]->setL(L_new);
    metadata[relB][colB]->setL(L_new);

    int F_new = (F_a * F_b) / n;
    metadata[relA][colA]->setF(F_new);
    metadata[relB][colB]->setF(F_new);

    int D_new = (D_a * D_b) / n;
    //cout << "D_new is " << D_new << endl;
    metadata[relA][colA]->setD(D_new);
    metadata[relB][colB]->setD(D_new);

//..............for the other columns.........................

    //set new values
    // L, U stay the same

    // for relA columns
    for (int i = 0; i < columns[relA]; i++){
        metadata[relA][i]->setF(F_new);

        int D_c = metadata[relA][i]->getD();
        int F_c = metadata[relA][i]->getF();

        metadata[relA][i]->setF(F_new);
        if (D_c != 0) metadata[relA][i]->setD(D_c * (1 - pow(1 - D_new / D_a, F_c / D_c)));
                
    }
    // for relB columns
    for (int i = 0; i < columns[relB]; i++){
        metadata[relB][i]->setF(F_new);

        int D_c = metadata[relB][i]->getD();
        int F_c = metadata[relB][i]->getF();

        metadata[relB][i]->setF(F_new);
        if (D_c != 0) metadata[relB][i]->setD(D_c * (1 - pow(1 - D_new / D_b, F_c / D_c)));
                
    }
    return F_new;
}



