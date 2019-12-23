//
// Created by jong on 11/18/19.
//

#include "ix.h"
#include "ix_test_util.h"

const int minusone = -1;
const int zero = 0;
const int one = 1;
const int two = 2;
const int three = 3;
const int four = 4;
const int five = 5;
const int six = 6;
const int seven = 7;
const int eight = 8;
const int nine = 9;
const int ten = 10;
const int eleven = 11;
const int twelve = 12;
const int thirteen = 13;
const int fourteen = 14;
const int fifteen = 15;
const int sixteen = 16;

const unsigned uzero = 0;
const unsigned uone = 1;
const unsigned utwo = 2;
const unsigned uthree = 3;
const unsigned ufour = 4;
const unsigned ufive = 5;
const unsigned usix = 6;
const unsigned useven = 7;
const unsigned ueight = 8;
const unsigned unine = 9;
const unsigned uten = 10;
const unsigned ueleven = 11;
const unsigned utwelve = 12;
const unsigned uthirteen = 13;
const unsigned ufourteen = 14;

Attribute testAttribute;
const std::string indexFileName = "ixtest96.ix";

void prepEntry(const char* key, const int cstrLen, const RID rid, void* actualKey) {
    memset(actualKey, 0, 1000);
    const char NULLVAL = '\0';
    const int valLen = 996;
    memcpy(actualKey, &valLen, 4);
    memcpy((byte*)actualKey + 4, key, cstrLen);
    memcpy((byte*)actualKey + 4 + cstrLen, &NULLVAL, cstrLen);
}

/* Understanding insertEntry */
int main() {
    RID rid;
    rid.pageNum = 99; rid.slotNum = 99;
    testAttribute.type = TypeVarChar;
    testAttribute.name = "LargeDummies";
    testAttribute.length = 1000;

    void* largeKey = malloc(1000);
    memset(largeKey, 0, 1000);
    
    indexManager.destroyFile(indexFileName);
    
    // create index file
    RC rc = indexManager.createFile(indexFileName);
    assert(rc == success && "indexManager::createFile() should not fail.");

    // open index file
    IXFileHandle ixFileHandle;
    rc = indexManager.openFile(indexFileName, ixFileHandle);
    assert(rc == success && "indexManager::openFile() should not fail.");

    /* Insert A */
    std::string stdstr = "A";
    prepEntry(stdstr.c_str(), (unsigned)stdstr.length(), rid, largeKey);
    rc = indexManager.insertEntry(ixFileHandle, testAttribute, largeKey, rid);

    /* Insert D */
    stdstr = "D";
    prepEntry(stdstr.c_str(), (unsigned)stdstr.length(), rid, largeKey);
    rc = indexManager.insertEntry(ixFileHandle, testAttribute, largeKey, rid);

    /* Insert C */
    stdstr = "C";
    prepEntry(stdstr.c_str(), (unsigned)stdstr.length(), rid, largeKey);
    rc = indexManager.insertEntry(ixFileHandle, testAttribute, largeKey, rid);

    /* Insert B */
    stdstr = "B";
    prepEntry(stdstr.c_str(), (unsigned)stdstr.length(), rid, largeKey);
    rc = indexManager.insertEntry(ixFileHandle, testAttribute, largeKey, rid);

    /* Insert E */
    stdstr = "E";
    prepEntry(stdstr.c_str(), (unsigned)stdstr.length(), rid, largeKey);
    rc = indexManager.insertEntry(ixFileHandle, testAttribute, largeKey, rid);

    /* Insert F */
    stdstr = "F";
    prepEntry(stdstr.c_str(), (unsigned)stdstr.length(), rid, largeKey);
    rc = indexManager.insertEntry(ixFileHandle, testAttribute, largeKey, rid);

    /* Insert G */
    stdstr = "G";
    prepEntry(stdstr.c_str(), (unsigned)stdstr.length(), rid, largeKey);
    rc = indexManager.insertEntry(ixFileHandle, testAttribute, largeKey, rid);
    
    indexManager.printBtreeDebug(ixFileHandle, testAttribute);

    /* Delete A */
    stdstr = "A";
    prepEntry(stdstr.c_str(), (unsigned)stdstr.length(), rid, largeKey);
    rc = indexManager.deleteEntry(ixFileHandle, testAttribute, largeKey, rid);
    assert(rc == 0);

    indexManager.printBtreeDebug(ixFileHandle, testAttribute);

    /* Delete B */
    stdstr = "B";
    prepEntry(stdstr.c_str(), (unsigned)stdstr.length(), rid, largeKey);
    rc = indexManager.deleteEntry(ixFileHandle, testAttribute, largeKey, rid);
    assert(rc == 0);

    indexManager.printBtreeDebug(ixFileHandle, testAttribute);

    /* Delete C */
    stdstr = "C";
    prepEntry(stdstr.c_str(), (unsigned)stdstr.length(), rid, largeKey);
    rc = indexManager.deleteEntry(ixFileHandle, testAttribute, largeKey, rid);
    assert(rc == 0);

    indexManager.printBtreeDebug(ixFileHandle, testAttribute);

    /* Delete D */
    stdstr = "D";
    prepEntry(stdstr.c_str(), (unsigned)stdstr.length(), rid, largeKey);
    rc = indexManager.deleteEntry(ixFileHandle, testAttribute, largeKey, rid);
    assert(rc == 0);

    indexManager.printBtreeDebug(ixFileHandle, testAttribute);

    /* Delete E */
    stdstr = "E";
    prepEntry(stdstr.c_str(), (unsigned)stdstr.length(), rid, largeKey);
    rc = indexManager.deleteEntry(ixFileHandle, testAttribute, largeKey, rid);
    assert(rc == 0);

    indexManager.printBtreeDebug(ixFileHandle, testAttribute);

    /* Delete F */
    stdstr = "F";
    prepEntry(stdstr.c_str(), (unsigned)stdstr.length(), rid, largeKey);
    rc = indexManager.deleteEntry(ixFileHandle, testAttribute, largeKey, rid);
    assert(rc == 0);

    /* Delete G */
    stdstr = "G";
    prepEntry(stdstr.c_str(), (unsigned)stdstr.length(), rid, largeKey);
    rc = indexManager.deleteEntry(ixFileHandle, testAttribute, largeKey, rid);
    assert(rc == 0);

    indexManager.printBtreeDebug(ixFileHandle, testAttribute);
}