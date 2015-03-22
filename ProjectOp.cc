#include "ProjectOp.h"

void* ProjectOpProcessor(void* data) {
    Pipe *inPipe= ((ProjectOpDataT*)data)->inPipe;
    Pipe *outPipe= ((ProjectOpDataT*)data)->outPipe;
    int *keepMe= ((ProjectOpDataT*)data)->keepMe;
    int numAttsInput= ((ProjectOpDataT*)data)->numAttsInput;
    int numAttsOutput= ((ProjectOpDataT*)data)->numAttsOutput;
    
    Record *fetchme=new Record();
    
    while(inPipe->Remove(fetchme) ) {
        fetchme->Project(keepMe, numAttsOutput, numAttsInput);
        outPipe->Insert(fetchme);
    }
    
    outPipe->ShutDown();
}

void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {
    ProjectOpDataT *opData = new ProjectOpDataT();
    opData->inPipe=&inPipe;
    opData->outPipe=&outPipe;
    opData->keepMe=keepMe;
    opData->numAttsInput=numAttsInput;
    opData->numAttsOutput=numAttsOutput;
    pthread_create(&thread, NULL, &ProjectOpProcessor, (void*)opData);
}

void Project::WaitUntilDone () {
    pthread_join(thread, NULL);
}

void Project::Use_n_Pages (int n) {
    
}