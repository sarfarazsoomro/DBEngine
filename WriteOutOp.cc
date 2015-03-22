#include "WriteOutOp.h"
#include "Defs.h"

void* WriteOutOpProcessor(void* data) {
    Pipe *inPipe= ((WriteOutOpDataT*)data)->inPipe;
    FILE *outFile= ((WriteOutOpDataT*)data)->outFile;
    Schema *mySchema= ((WriteOutOpDataT*)data)->mySchema;

    Record fetchme;
    while( inPipe->Remove(&fetchme)==1 ) {
        fputs(fetchme.ToString(mySchema), outFile);
    }
}

void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) {
    WriteOutOpDataT *opData = new WriteOutOpDataT();
    opData->inPipe=&inPipe;
    opData->outFile=outFile;
    opData->mySchema=&mySchema;
    pthread_create(&thread, NULL, &WriteOutOpProcessor, (void*)opData);
}

void WriteOut::WaitUntilDone () {
    pthread_join(thread, NULL);
}

void WriteOut::Use_n_Pages (int n) {
    
}