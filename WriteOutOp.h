#ifndef REL_OP_WRITEOUT_H
#define REL_OP_WRITEOUT_H

#include "RelOp.h"

typedef struct WriteOutOpData {
    Pipe *inPipe;
    FILE *outFile;
    Schema *mySchema;
} WriteOutOpDataT;

class WriteOut : public RelationalOp {
    private:
    pthread_t thread;
    
    public:
    void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
    void WaitUntilDone ();
    void Use_n_Pages (int n);
};
#endif