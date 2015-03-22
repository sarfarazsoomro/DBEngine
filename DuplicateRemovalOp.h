#ifndef REL_OP_DUPLICATEREMOVAL_H
#define REL_OP_DUPLICATEREMOVAL_H

#include "RelOp.h"

typedef struct DuplicateRemovalOpData {
    Pipe *inPipe;
    Pipe *outPipe;
    Schema *mySchema;
    int runLen;
} DuplicateRemovalOpDataT;

class DuplicateRemoval : public RelationalOp {
    private:
    pthread_t thread;
    int runLen;
    
    public:
    void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
    void WaitUntilDone ();
    void Use_n_Pages (int n);
};
#endif