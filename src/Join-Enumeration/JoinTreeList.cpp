#include "JoinTreeList.hpp"

//-------------------------JoinTree---------------------------------------

JoinTree::JoinTree(Predicates* p){
    cout << "New join tree of size-1 predicates was just created!" << endl;
    arr = new Predicates*[1];
    size = 1;
    cost = nullptr;
    arr[0] = p;
}

JoinTree::JoinTree(Predicates** p, int size, Predicates* newR){
    arr = new Predicates*[size + 1];
    this->size = size + 1;
    cost = nullptr;
    for (int i = 0; i < size; i++){
        arr[i] = p[i];
    }
    arr[size] = newR; 
}

void JoinTree::print(){
    for (int i = 0; i < size; i++){
        cout << arr[i]->relation_left << " ";
    }
    cout << endl;
}

//-------------------------JoinTreeNode---------------------------------------

JoinTreeNode::JoinTreeNode(JoinTree* jt, int i){
    this->jt = jt;
    this->index = i;
    next = nullptr; 
}

//-------------------------JoinTreeList---------------------------------------
JoinTreeList::JoinTreeList(){
    head = nullptr;
    size = 0;
}


JoinTreeNode* JoinTreeList::getHead(){return head;}

void JoinTreeList::add(JoinTree* jt){
    int index = 0;
    if (head == nullptr){
        head = new JoinTreeNode(jt, index); 
        return;
    }
    JoinTreeNode* temp = head;
    while (temp->next != nullptr){
        temp = temp->next;
        index++;
    }
    temp->next = new JoinTreeNode(jt, index);
    size++;
}


JoinTreeNode* JoinTreeList::contains(Predicates** p, int size){
    bool flag = true;
    JoinTreeNode* temp = head;
    while (temp != nullptr){
        for (int i = 0; i < size; i++){
            if (temp->jt->arr[i] != p[i]){
                flag = false;
                break;
            }
        }
        if (flag == true) return temp;
        temp->next;
    }
    if (flag == false) return nullptr;    
}

void JoinTreeList::replace(JoinTreeNode* old, JoinTree* newJ){
    if (old == nullptr){
        add(newJ);
    }
    JoinTreeNode* temp = head;
    while(temp != nullptr){
        if (temp->index == old->index){
            delete temp->jt;
            temp->jt = newJ;
        }
    }
}

void JoinTreeList::print(){
    JoinTreeNode* temp = head;
    while (temp != nullptr){
        cout << "Join tree with prdcts";
        temp->jt->print();
        cout << endl;
        temp = temp->next;
    }

}