#ifndef REL_OP_SELECTPIPE_H
#define REL_OP_SELECTPIPE_H

#include "RelOp.h"

typedef struct SelectPipeOpData {
    Pipe *inPipe;
    Pipe *outPipe;
    CNF *cnf;
    Record *rec;
} SelectPipeOpDataT;

class SelectPipe : public RelationalOp {
	private:
        pthread_t thread;
        
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};

#endif