#ifndef REL_OP_PROJECT_H
#define REL_OP_PROJECT_H

#include "RelOp.h"

typedef struct ProjectOpData {
    Pipe *inPipe;
    Pipe *outPipe;
    int *keepMe;
    int numAttsInput;
    int numAttsOutput;
} ProjectOpDataT;

class Project : public RelationalOp { 
    private:
    pthread_t thread;
    
    public:
    void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
    void WaitUntilDone ();
    void Use_n_Pages (int n);
};

#endif