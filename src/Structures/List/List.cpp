#include "../Structures.h"

List::List(){
  head = new Node(NULL);
  size = 0;
}

void List::Push(MatchRow* row){
  Node* tmp = head->next;
  head->next = new Node(row);
  head->next->next = tmp;
  size++;
}

void List::Pop(){
  Node* tmp = head;
  head = head->next;
  delete tmp->row;
  delete tmp;
  size--;
}

void List::Delete(Node* l){
  Node* tmp = l->next;
  l->next = l->next->next;
  delete tmp->row;
  delete tmp;
  size--;
}

List::~List(){
  while(head != NULL)
    Pop();
}
