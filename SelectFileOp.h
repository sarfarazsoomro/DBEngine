#ifndef REL_OP_SELECTFILE_H
#define REL_OP_SELECTFILE_H

#include "RelOp.h"

typedef struct SelectFileOpData {
    DBFile *inFile;
    Pipe *outPipe;
    CNF *cnf;
    Record *rec;
} SelectFileOpDataT;

class SelectFile : public RelationalOp {
	private:
        pthread_t thread;
	// Record *buffer;
        //int buffSize;
	
        public:
	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};

#endif