#include "DuplicateRemovalOp.h"

void* DuplicateRemovalOpProcessor(void* data) {
    //init data
    Pipe *inPipe= ((DuplicateRemovalOpDataT*)data)->inPipe;
    Pipe *outPipe= ((DuplicateRemovalOpDataT*)data)->outPipe;
    Schema *mySchema= ((DuplicateRemovalOpDataT*)data)->mySchema;
    int runLen= ((DuplicateRemovalOpDataT*)data)->runLen;
    
    
    OrderMaker *om= new OrderMaker(mySchema);
    
    Pipe *bqOut=new Pipe(1000);
    
    BigQ *bq=new BigQ(*inPipe, *bqOut, *om, runLen);
   
    Record left,right;
    //first record
    int result=bqOut->Remove(&left);
    //goes straight into output
    if( result==1 ) {
        Record copy;
        copy.Copy(&left);
        outPipe->Insert(&copy);
    }
    //second record
    result=bqOut->Remove(&right);
    //right.Print(mySchema);
    ComparisonEngine ce;
    int compr=0;
    while( result==1 ) {
        //if both (this and last, OR right and left) the records are not equal
        //means right is not identical
        //right.Print(mySchema);
        compr=ce.Compare(&left, &right, om);
        if( compr!=0 ) {
            Record copy;
            copy.Copy(&right);
            outPipe->Insert(&copy);
            left.Consume(&right);
        }
        result=bqOut->Remove(&right);
    }
    
    //take care of the last record which terminated the above loop
    if( ce.Compare(&left, &right, om)!=0 ) {
        Record copy;
        copy.Copy(&right);
        outPipe->Insert(&copy);
    }
    outPipe->ShutDown();
}

void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {
    runLen=1;
    DuplicateRemovalOpDataT *opData = new DuplicateRemovalOpDataT();
    opData->inPipe=&inPipe;
    opData->outPipe=&outPipe;
    opData->mySchema=&mySchema;
    opData->runLen=runLen;
    pthread_create(&thread, NULL, &DuplicateRemovalOpProcessor, (void*)opData);
}

void DuplicateRemoval::WaitUntilDone () {
    pthread_join(thread, NULL);
}

void DuplicateRemoval::Use_n_Pages (int n) {
    runLen=n;
}