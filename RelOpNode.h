#ifndef RON_H
#define RON_H
#include <iostream>
#include <stdlib.h>
#include "RelOp.cc"

using namespace std;

class RelOpNode {
private:
public:
    string nodeType;
    RelationalOp *op;
    RelOpNode *left;
    RelOpNode *right;    
    RelOpNode ();
    RelOpNode (string type);
};
#endif