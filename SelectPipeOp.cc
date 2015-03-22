#include "SelectPipeOp.h"

void* SelectPipeProcessor(void* data) {
    Pipe *inPipe= ((SelectPipeOpDataT*)data)->inPipe;
    Pipe *outPipe= ((SelectPipeOpDataT*)data)->outPipe;
    CNF *selOp=((SelectPipeOpDataT*)data)->cnf;
    Record *literal=((SelectPipeOpDataT*)data)->rec;
    
    Record fetchme;
    ComparisonEngine ce;
    
    while(inPipe->Remove(&fetchme) ) {
        if( ce.Compare(&fetchme, literal, selOp) ) {
            outPipe->Insert(&fetchme);
        }
    }
    
    outPipe->ShutDown();
}

void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
    SelectPipeOpDataT *opData = new SelectPipeOpDataT();
    opData->inPipe=&inPipe;
    opData->outPipe=&outPipe;
    opData->cnf=&selOp;
    opData->rec=&literal;
    pthread_create(&thread, NULL, &SelectPipeProcessor, (void*)opData);
}

void SelectPipe::WaitUntilDone () {
    pthread_join (thread, NULL);
}

void SelectPipe::Use_n_Pages (int runlen) {
}