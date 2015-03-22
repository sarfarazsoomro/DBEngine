#ifndef REL_OP_SUM_H
#define REL_OP_SUM_H

#include "RelOp.h"

typedef struct SumOpData {
    Pipe *inPipe;
    Pipe *outPipe;
    Function *computeMe;
} SumOpDataT;

class Sum : public RelationalOp {
    private:
    pthread_t thread;
    
    public:
    void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
    void WaitUntilDone ();
    void Use_n_Pages (int n);
};
#endif