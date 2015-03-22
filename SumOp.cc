#include "ProjectOp.h"
#include "SumOp.h"

void* SumOpProcessor(void* data) {
    Pipe *inPipe= ((SumOpDataT*)data)->inPipe;
    Pipe *outPipe= ((SumOpDataT*)data)->outPipe;
    Function *computeMe= ((SumOpDataT*)data)->computeMe;
    
    Record *fetchme=new Record();
    double sum=0;
    int ival=0;
    double dval=0;
    
    int count=0;
    
    while(inPipe->Remove(fetchme) ) {
        count++;
        computeMe->Apply(*fetchme, ival, dval);
        sum += (ival + dval);
    }
    
    Attribute DA = {"double", Double};
    Schema out_sch ("out_sch", 1, &DA);
    
    fetchme->ComposeRecord(&out_sch, (NumberToString<double>(sum)+std::string("|")).c_str() );
    
    outPipe->Insert(fetchme);
    
    outPipe->ShutDown();
}

void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe) {
    SumOpDataT *opData = new SumOpDataT();
    opData->inPipe=&inPipe;
    opData->outPipe=&outPipe;
    opData->computeMe=&computeMe;
    pthread_create(&thread, NULL, &SumOpProcessor, (void*)opData);
}

void Sum::WaitUntilDone () {
    pthread_join(thread, NULL);
}

void Sum::Use_n_Pages (int n) {
    
}