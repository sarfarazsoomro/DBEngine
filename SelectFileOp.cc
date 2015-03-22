#include "SelectFileOp.h"

void* SelectFileProcessor(void* data) {
    DBFile *inFile= ((SelectFileOpDataT*)data)->inFile;
    Pipe *outPipe= ((SelectFileOpDataT*)data)->outPipe;
    CNF *selOp=((SelectFileOpDataT*)data)->cnf;
    Record *literal=((SelectFileOpDataT*)data)->rec;
    
    Record fetchme;
    ComparisonEngine ce;
    int count=0;
    while(inFile->GetNext(fetchme)==1) {
        if( ce.Compare(&fetchme, literal, selOp) ) {
            count++;
            outPipe->Insert(&fetchme);
        }
    }
    outPipe->ShutDown();
}

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
    SelectFileOpDataT *opData = new SelectFileOpDataT();
    opData->inFile=&inFile;
    opData->outPipe=&outPipe;
    opData->cnf=&selOp;
    opData->rec=&literal;
    pthread_create(&thread, NULL, &SelectFileProcessor, (void*)opData);
}

void SelectFile::WaitUntilDone () {
    pthread_join (thread, NULL);
}

void SelectFile::Use_n_Pages (int runlen) {
}