#ifndef REL_OP_GROUPBY_H
#define REL_OP_GROUPBY_H

#include "RelOp.h"

typedef struct GroupByOpData {
    Pipe *inPipe;
    Pipe *outPipe;
    Function *computeMe;
    OrderMaker *groupAtts;
    int runLen;
} GroupByOpDataT;

class GroupBy : public RelationalOp {
    private:
    pthread_t thread;
    int runLen;
    
    public:
    void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
    void WaitUntilDone ();
    void Use_n_Pages (int n);
};
#endif