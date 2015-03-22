#ifndef REL_OP_JOIN_H
#define REL_OP_JOIN_H

#include "RelOp.h"

typedef struct JoinOpData {
    Pipe *inPipeL;
    Pipe *inPipeR;
    Pipe *outPipe;
    CNF *selOp;
    Record *literal;
    int runLen;
} JoinOpDataT;

class Join : public RelationalOp {
	private:
        pthread_t thread;
	// Record *buffer;
        int runLen;
	
        public:
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};

#endif