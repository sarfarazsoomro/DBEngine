#include "GroupByOp.h"

void* GroupByOpProcessor(void* data) {
    Pipe *inPipe= ((GroupByOpDataT*)data)->inPipe;
    Pipe *outPipe= ((GroupByOpDataT*)data)->outPipe;
    OrderMaker *groupAtts = ((GroupByOpDataT*)data)->groupAtts;
    Function *computeMe= ((GroupByOpDataT*)data)->computeMe;
    int runLen= ((GroupByOpDataT*)data)->runLen;
    
    Pipe *bqOut=new Pipe(1000);
    BigQ *bq=new BigQ(*inPipe, *bqOut, *groupAtts, runLen);
    
    Record *fetchme= new Record();
    Record oldRecord;
    
    ComparisonEngine ce;
    
    double sum=0;
    int ival=0;
    double dval=0;
    
    //creating the attToKeep array for merged attribute
    //will be used to merge the sum record with the group attributes
    int numGrpAtts=groupAtts->GetNumAtts();
    int *mergeAtts=new int[1+numGrpAtts];
    int *groupAttsArr=groupAtts->GetAtts();
    
    mergeAtts[0]=0;
    for(int i=1; i<=numGrpAtts; i++) {
        mergeAtts[i]=groupAttsArr[i-1];
    }
    
    bqOut->GetNext(&oldRecord);
    
    computeMe->Apply(oldRecord, ival, dval);
    sum += (ival + dval);
        
    Attribute DA = {"double", Double};
    Schema out_sch ("out_sch", 1, &DA);
    
    Record composedRecord;
    Record mergedRecord;
    int count=0;
    while(bqOut->Remove(fetchme) ) {
        if( ce.Compare(&oldRecord, fetchme, groupAtts)==0 ) {
            computeMe->Apply(*fetchme, ival, dval);
            sum += (ival + dval);
            oldRecord.Copy(fetchme);
        } else {
            composedRecord.ComposeRecord(&out_sch, (NumberToString<double>(sum)+std::string("|")).c_str());
            mergedRecord.MergeRecords(&composedRecord, &oldRecord, 1, oldRecord.GetNumAtts(), mergeAtts, 1+numGrpAtts, 1);
            outPipe->Insert(&mergedRecord);
            sum=0;
            ival=0;
            dval=0;
            computeMe->Apply(*fetchme, ival, dval);
            sum += (ival + dval);
            oldRecord.Copy(fetchme);            
        }
    }
    composedRecord.ComposeRecord(&out_sch, (NumberToString<double>(sum)+std::string("|")).c_str());
    mergedRecord.MergeRecords(&composedRecord, fetchme, 1, fetchme->GetNumAtts(), mergeAtts, 1+numGrpAtts, 1);
    outPipe->Insert(&mergedRecord);
    outPipe->ShutDown();
}

void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) {
    GroupByOpDataT *opData = new GroupByOpDataT();
    opData->inPipe=&inPipe;
    opData->outPipe=&outPipe;
    opData->groupAtts=&groupAtts;
    opData->computeMe=&computeMe;
    opData->runLen=runLen;
    pthread_create(&thread, NULL, &GroupByOpProcessor, (void*)opData);
}

void GroupBy::WaitUntilDone () {
    pthread_join(thread, NULL);
}

void GroupBy::Use_n_Pages (int n) {
    runLen=n;
}