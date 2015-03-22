#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include <algorithm>
#include <vector>
#include <queue>

using namespace std;

class BigQ {
private:
    OrderMaker sort;
    pthread_t t1;
public:

	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
        void WaitUntilDone();
};

#endif
