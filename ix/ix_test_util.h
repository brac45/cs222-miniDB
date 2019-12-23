#ifndef _ix_test_util_h_
#define _ix_test_util_h_

#ifndef _fail_
#define _fail_
const int fail = -1;
#endif

#include "../rbf/test_util.h"

IndexManager &indexManager = IndexManager::instance();

byte readIsLeaf(byte* buf) {
    byte isLeaf = 0;
    memcpy(&isLeaf, buf, 1);

    return isLeaf;
}

unsigned readN(byte* buf) {
    unsigned N = 0;
    memcpy(&N, buf+1, 4);
    return N;
}

unsigned readF(byte* buf) {
    unsigned F = 0;
    memcpy(&F, buf+5, 4);
    return F;
}

int checkIntValue(void* key) {
    int retVal = 0;
    memcpy(&retVal, key, 4);
    return retVal;
}

float checkRealValue(void* key) {
    float retVal = 0;
    memcpy(&retVal, key, 4);
    return retVal;
}

#endif


