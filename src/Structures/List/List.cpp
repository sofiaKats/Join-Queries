#include "../Structures.hpp";

void List::Push(MatchRow* row){
  Node* tmp = head;
  head = new Node(row);
  head->next = tmp;
}

void List::Pop(){
  Node* tmp = head;
  head = head->next;
  delete tmp;
}

void List::Delete(){

}

List::~List(){
  while(head != NULL)
    Pop();
}
