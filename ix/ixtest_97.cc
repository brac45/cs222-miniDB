//
// Created by jong on 11/17/19.
//

#include "ix.h"
#include "ix_test_util.h"

const byte LEAF = 1;
const byte INTER = 0;
const unsigned N = 3;
const unsigned HEADER_SIZE = 9;
const unsigned INTER_SIZE = 61;
const unsigned LEAF_SIZE = 37;
const unsigned ROOT_SIZE = 29;

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

const std::string indexFileName = "custom_ix_test.ix";

Attribute testAttribute;

void fillRootHeader(byte* buffer) {
    unsigned F = PAGE_SIZE - ROOT_SIZE;
    unsigned rootN = 1;
    memcpy(buffer, &INTER, 1);
    memcpy(buffer+1, &rootN, 4);
    memcpy(buffer+5, &F, 4);
}

void fillLeafHeader(byte* buffer) {
    unsigned F = PAGE_SIZE - LEAF_SIZE;
    unsigned M = 2;
    memcpy(buffer, &LEAF, 1);
    memcpy(buffer+1, &M, 4);
    memcpy(buffer+5, &F, 4);
}

void fillInterHeader(byte* buffer) {
    unsigned F = PAGE_SIZE - INTER_SIZE;
    memcpy(buffer, &INTER, 1);
    memcpy(buffer+1, &N, 4);
    memcpy(buffer+5, &F, 4);
}

void buildBtree() {
    IndexManager &ixManager = IndexManager::instance();
    indexManager.destroyFile(indexFileName);

    // create index file
    RC rc = indexManager.createFile(indexFileName);
    assert(rc == success && "indexManager::createFile() should not fail.");

    // open index file
    IXFileHandle ixFileHandle;
    rc = indexManager.openFile(indexFileName, ixFileHandle);
    assert(rc == success && "indexManager::openFile() should not fail.");

    RID rid;
    rid.pageNum = 99; rid.slotNum = 99;
    byte buffer[PAGE_SIZE] = {0};
    fillRootHeader(buffer);

    /* Build root node */
    ixFileHandle.init = 0;
    ixFileHandle.rootPageNum = 0;

    int offset = 9;
    memcpy(buffer + offset, &uone, 4); offset += 4;
    memcpy(buffer + offset, &nine, 4); offset += 4;
    memcpy(buffer + offset, &rid.pageNum, 4); offset += 4;
    memcpy(buffer + offset, &rid.slotNum, 4); offset += 4;
    memcpy(buffer + offset, &utwo, 4); offset += 4;
    assert(offset == ROOT_SIZE);

    ixFileHandle.appendPage(buffer);
    memset(buffer, 0, PAGE_SIZE);
    ix_helpers::UpdateVariables(ixFileHandle);

    /* Build first intermediate node */
    fillInterHeader(buffer);
    offset = 9;
    memcpy(buffer + offset, &uthree, 4); offset += 4;

    memcpy(buffer + offset, &three, 4); offset += 4;
    memcpy(buffer + offset, &rid.pageNum, 4); offset += 4;
    memcpy(buffer + offset, &rid.slotNum, 4); offset += 4;
    memcpy(buffer + offset, &ufour, 4); offset += 4;

    memcpy(buffer + offset, &five, 4); offset += 4;
    memcpy(buffer + offset, &rid.pageNum, 4); offset += 4;
    memcpy(buffer + offset, &rid.slotNum, 4); offset += 4;
    memcpy(buffer + offset, &ufive, 4); offset += 4;

    memcpy(buffer + offset, &seven, 4); offset += 4;
    memcpy(buffer + offset, &rid.pageNum, 4); offset += 4;
    memcpy(buffer + offset, &rid.slotNum, 4); offset += 4;
    memcpy(buffer + offset, &usix, 4); offset += 4;
    assert(offset == INTER_SIZE);

    ixFileHandle.appendPage(buffer);
    memset(buffer, 0, PAGE_SIZE);

    /* Build second intermediate node */
    fillInterHeader(buffer);
    offset = 9;
    memcpy(buffer + offset, &useven, 4); offset += 4;

    memcpy(buffer + offset, &eleven, 4); offset += 4;
    memcpy(buffer + offset, &rid.pageNum, 4); offset += 4;
    memcpy(buffer + offset, &rid.slotNum, 4); offset += 4;
    memcpy(buffer + offset, &ueight, 4); offset += 4;

    memcpy(buffer + offset, &thirteen, 4); offset += 4;
    memcpy(buffer + offset, &rid.pageNum, 4); offset += 4;
    memcpy(buffer + offset, &rid.slotNum, 4); offset += 4;
    memcpy(buffer + offset, &unine, 4); offset += 4;

    memcpy(buffer + offset, &fifteen, 4); offset += 4;
    memcpy(buffer + offset, &rid.pageNum, 4); offset += 4;
    memcpy(buffer + offset, &rid.slotNum, 4); offset += 4;
    memcpy(buffer + offset, &uten, 4); offset += 4;
    assert(offset == INTER_SIZE);

    ixFileHandle.appendPage(buffer);
    memset(buffer, 0, PAGE_SIZE);

    /* Build first leaf node */
    fillLeafHeader(buffer);
    offset = 9;
    memcpy(buffer + offset, &ufour, 4); offset += 4;

    memcpy(buffer + offset, &one, 4); offset += 4;
    memcpy(buffer + offset, &rid.pageNum, 4); offset += 4;
    memcpy(buffer + offset, &rid.slotNum, 4); offset += 4;
    memcpy(buffer + offset, &two, 4); offset += 4;
    memcpy(buffer + offset, &rid.pageNum, 4); offset += 4;
    memcpy(buffer + offset, &rid.slotNum, 4); offset += 4;
    assert(offset == LEAF_SIZE);

    ixFileHandle.appendPage(buffer);
    memset(buffer, 0, PAGE_SIZE);

    /* Build second leaf node */
    fillLeafHeader(buffer);
    offset = 9;
    memcpy(buffer + offset, &ufive, 4); offset += 4;

    memcpy(buffer + offset, &three, 4); offset += 4;
    memcpy(buffer + offset, &rid.pageNum, 4); offset += 4;
    memcpy(buffer + offset, &rid.slotNum, 4); offset += 4;
    memcpy(buffer + offset, &four, 4); offset += 4;
    memcpy(buffer + offset, &rid.pageNum, 4); offset += 4;
    memcpy(buffer + offset, &rid.slotNum, 4); offset += 4;
    assert(offset == LEAF_SIZE);

    ixFileHandle.appendPage(buffer);
    memset(buffer, 0, PAGE_SIZE);

    /* Build third leaf node */
    fillLeafHeader(buffer);
    offset = 9;
    memcpy(buffer + offset, &usix, 4); offset += 4;

    memcpy(buffer + offset, &five, 4); offset += 4;
    memcpy(buffer + offset, &rid.pageNum, 4); offset += 4;
    memcpy(buffer + offset, &rid.slotNum, 4); offset += 4;
    memcpy(buffer + offset, &six, 4); offset += 4;
    memcpy(buffer + offset, &rid.pageNum, 4); offset += 4;
    memcpy(buffer + offset, &rid.slotNum, 4); offset += 4;
    assert(offset == LEAF_SIZE);

    ixFileHandle.appendPage(buffer);
    memset(buffer, 0, PAGE_SIZE);

    /* Build fourth leaf node */
    fillLeafHeader(buffer);
    offset = 9;
    memcpy(buffer + offset, &useven, 4); offset += 4;

    memcpy(buffer + offset, &seven, 4); offset += 4;
    memcpy(buffer + offset, &rid.pageNum, 4); offset += 4;
    memcpy(buffer + offset, &rid.slotNum, 4); offset += 4;
    memcpy(buffer + offset, &eight, 4); offset += 4;
    memcpy(buffer + offset, &rid.pageNum, 4); offset += 4;
    memcpy(buffer + offset, &rid.slotNum, 4); offset += 4;
    assert(offset == LEAF_SIZE);

    ixFileHandle.appendPage(buffer);
    memset(buffer, 0, PAGE_SIZE);

    /* Build fifth leaf node */
    fillLeafHeader(buffer);
    offset = 9;
    memcpy(buffer + offset, &ueight, 4); offset += 4;

    memcpy(buffer + offset, &nine, 4); offset += 4;
    memcpy(buffer + offset, &rid.pageNum, 4); offset += 4;
    memcpy(buffer + offset, &rid.slotNum, 4); offset += 4;
    memcpy(buffer + offset, &ten, 4); offset += 4;
    memcpy(buffer + offset, &rid.pageNum, 4); offset += 4;
    memcpy(buffer + offset, &rid.slotNum, 4); offset += 4;
    assert(offset == LEAF_SIZE);

    ixFileHandle.appendPage(buffer);
    memset(buffer, 0, PAGE_SIZE);

    /* Build sixth leaf node */
    fillLeafHeader(buffer);
    offset = 9;
    memcpy(buffer + offset, &unine, 4); offset += 4;

    memcpy(buffer + offset, &eleven, 4); offset += 4;
    memcpy(buffer + offset, &rid.pageNum, 4); offset += 4;
    memcpy(buffer + offset, &rid.slotNum, 4); offset += 4;
    memcpy(buffer + offset, &twelve, 4); offset += 4;
    memcpy(buffer + offset, &rid.pageNum, 4); offset += 4;
    memcpy(buffer + offset, &rid.slotNum, 4); offset += 4;
    assert(offset == LEAF_SIZE);

    ixFileHandle.appendPage(buffer);
    memset(buffer, 0, PAGE_SIZE);

    /* Build seventh leaf node */
    fillLeafHeader(buffer);
    offset = 9;
    memcpy(buffer + offset, &uten, 4); offset += 4;

    memcpy(buffer + offset, &thirteen, 4); offset += 4;
    memcpy(buffer + offset, &rid.pageNum, 4); offset += 4;
    memcpy(buffer + offset, &rid.slotNum, 4); offset += 4;
    memcpy(buffer + offset, &fourteen, 4); offset += 4;
    memcpy(buffer + offset, &rid.pageNum, 4); offset += 4;
    memcpy(buffer + offset, &rid.slotNum, 4); offset += 4;
    assert(offset == LEAF_SIZE);

    ixFileHandle.appendPage(buffer);
    memset(buffer, 0, PAGE_SIZE);

    /* Build eighth leaf node */
    fillLeafHeader(buffer);
    offset = 9;
    memcpy(buffer + offset, &minusone, 4); offset += 4;

    memcpy(buffer + offset, &fifteen, 4); offset += 4;
    memcpy(buffer + offset, &rid.pageNum, 4); offset += 4;
    memcpy(buffer + offset, &rid.slotNum, 4); offset += 4;
    memcpy(buffer + offset, &sixteen, 4); offset += 4;
    memcpy(buffer + offset, &rid.pageNum, 4); offset += 4;
    memcpy(buffer + offset, &rid.slotNum, 4); offset += 4;
    assert(offset == LEAF_SIZE);

    ixFileHandle.appendPage(buffer);
    memset(buffer, 0, PAGE_SIZE);

    indexManager.printBtreeDebug(ixFileHandle,testAttribute);

    // close index file
    rc = indexManager.closeFile(ixFileHandle);
    assert(rc == success && "indexManager::closeFile() should not fail.");
}

void testScanMinusInfPlusInf() {
    // open index file
    IXFileHandle ixFileHandle;
    RC rc = indexManager.openFile(indexFileName, ixFileHandle);
    assert(rc == success && "indexManager::openFile() should not fail.");

    // Conduct a scan
    IX_ScanIterator ix_ScanIterator;
    rc = indexManager.scan(ixFileHandle, testAttribute, NULL, NULL, true, true, ix_ScanIterator);
    assert(rc == success && "indexManager::scan() should not fail.");

    // There should be one record
    int count = 0; RID rid;
    int key = 200;
    while (ix_ScanIterator.getNextEntry(rid, &key) == success) {
        std::cout << key << ", "; std::cout.flush();
        count++;
    }
    std::cout << std::endl;
    assert(count == 16 && "scan count is not correct.");

    ix_ScanIterator.close();
}

void testScanloInclusive() {
    // open index file
    IXFileHandle ixFileHandle;
    RC rc = indexManager.openFile(indexFileName, ixFileHandle);
    assert(rc == success && "indexManager::openFile() should not fail.");

    // Set lowkey
    void* lowkey = malloc(4);
    memcpy(lowkey, &seven, 4);

    // Conduct a scan
    IX_ScanIterator ix_ScanIterator;
    rc = indexManager.scan(ixFileHandle, testAttribute, lowkey, NULL, true, true, ix_ScanIterator);
    assert(rc == success && "indexManager::scan() should not fail.");

    free(lowkey);

    // There should be one record
    int count = 0; RID rid;
    int key = 200;
    while (ix_ScanIterator.getNextEntry(rid, &key) == success) {
        std::cout << key << ", "; std::cout.flush();
        count++;
    }
    std::cout << std::endl;
    assert(count == 10 && "scan count is not correct.");

    ix_ScanIterator.close();
}

void testScanloExclusive() {
    // open index file
    IXFileHandle ixFileHandle;
    RC rc = indexManager.openFile(indexFileName, ixFileHandle);
    assert(rc == success && "indexManager::openFile() should not fail.");

    // Set lowkey
    void* lowkey = malloc(4);
    memcpy(lowkey, &seven, 4);

    // Conduct a scan
    IX_ScanIterator ix_ScanIterator;
    rc = indexManager.scan(ixFileHandle, testAttribute, lowkey, NULL, false, true, ix_ScanIterator);
    assert(rc == success && "indexManager::scan() should not fail.");

    free(lowkey);

    // There should be one record
    int count = 0; RID rid;
    int key = 200;
    while (ix_ScanIterator.getNextEntry(rid, &key) == success) {
        std::cout << key << ", "; std::cout.flush();
        count++;
    }
    std::cout << std::endl;
    assert(count == 9 && "scan count is not correct.");

    ix_ScanIterator.close();
}

void testScanloInclusiveEOP() {
    // open index file
    IXFileHandle ixFileHandle;
    RC rc = indexManager.openFile(indexFileName, ixFileHandle);
    assert(rc == success && "indexManager::openFile() should not fail.");

    // Set lowkey
    void* lowkey = malloc(4);
    memcpy(lowkey, &eight, 4);

    // Conduct a scan
    IX_ScanIterator ix_ScanIterator;
    rc = indexManager.scan(ixFileHandle, testAttribute, lowkey, NULL, true, true, ix_ScanIterator);
    assert(rc == success && "indexManager::scan() should not fail.");

    free(lowkey);

    // There should be one record
    int count = 0; RID rid;
    int key = 200;
    while (ix_ScanIterator.getNextEntry(rid, &key) == success) {
        std::cout << key << ", "; std::cout.flush();
        count++;
    }
    std::cout << std::endl;
    assert(count == 9 && "scan count is not correct.");

    ix_ScanIterator.close();
}

void testScanloExclusiveEOP() {
    // open index file
    IXFileHandle ixFileHandle;
    RC rc = indexManager.openFile(indexFileName, ixFileHandle);
    assert(rc == success && "indexManager::openFile() should not fail.");

    // Set lowkey
    void* lowkey = malloc(4);
    memcpy(lowkey, &eight, 4);

    // Conduct a scan
    IX_ScanIterator ix_ScanIterator;
    rc = indexManager.scan(ixFileHandle, testAttribute, lowkey, NULL, false, true, ix_ScanIterator);
    assert(rc == success && "indexManager::scan() should not fail.");

    free(lowkey);

    // There should be one record
    int count = 0; RID rid;
    int key = 200;
    while (ix_ScanIterator.getNextEntry(rid, &key) == success) {
        std::cout << key << ", "; std::cout.flush();
        count++;
    }
    std::cout << std::endl;
    assert(count == 8 && "scan count is not correct.");

    ix_ScanIterator.close();
}

void testScanHighInclusive() {
    // open index file
    IXFileHandle ixFileHandle;
    RC rc = indexManager.openFile(indexFileName, ixFileHandle);
    assert(rc == success && "indexManager::openFile() should not fail.");

    // Set lowkey
    void* highkey = malloc(4);
    memcpy(highkey, &sixteen, 4);

    // Conduct a scan
    IX_ScanIterator ix_ScanIterator;
    rc = indexManager.scan(ixFileHandle, testAttribute, NULL, highkey, false, true, ix_ScanIterator);
    assert(rc == success && "indexManager::scan() should not fail.");

    free(highkey);

    // There should be one record
    int count = 0; RID rid;
    int key = 200;
    while (ix_ScanIterator.getNextEntry(rid, &key) == success) {
        std::cout << key << ", "; std::cout.flush();
        count++;
    }
    std::cout << std::endl;
    assert(count == 16 && "scan count is not correct.");

    ix_ScanIterator.close();
}

void testScanHighExclusive() {
    // open index file
    IXFileHandle ixFileHandle;
    RC rc = indexManager.openFile(indexFileName, ixFileHandle);
    assert(rc == success && "indexManager::openFile() should not fail.");

    // Set lowkey
    void* highkey = malloc(4);
    memcpy(highkey, &sixteen, 4);

    // Conduct a scan
    IX_ScanIterator ix_ScanIterator;
    rc = indexManager.scan(ixFileHandle, testAttribute, NULL, highkey, false, false, ix_ScanIterator);
    assert(rc == success && "indexManager::scan() should not fail.");

    free(highkey);

    // There should be one record
    int count = 0; RID rid;
    int key = 200;
    while (ix_ScanIterator.getNextEntry(rid, &key) == success) {
        std::cout << key << ", "; std::cout.flush();
        count++;
    }
    std::cout << std::endl;
    assert(count == 15 && "scan count is not correct.");

    ix_ScanIterator.close();
}

void testScanBoth() {
    // open index file
    IXFileHandle ixFileHandle;
    RC rc = indexManager.openFile(indexFileName, ixFileHandle);
    assert(rc == success && "indexManager::openFile() should not fail.");

    // Set lowkey
    void* lowkey = malloc(4);
    memcpy(lowkey, &eight, 4);

    // Set hikey
    void* highkey = malloc(4);
    memcpy(highkey, &twelve, 4);

    // Conduct a scan
    IX_ScanIterator ix_ScanIterator;
    rc = indexManager.scan(ixFileHandle, testAttribute, lowkey, highkey, false, false, ix_ScanIterator);
    assert(rc == success && "indexManager::scan() should not fail.");

    free(highkey);

    // There should be one record
    int count = 0; RID rid;
    int key = 200;
    while (ix_ScanIterator.getNextEntry(rid, &key) == success) {
        std::cout << key << ", "; std::cout.flush();
        count++;
    }
    std::cout << std::endl;
    assert(count == 3 && "scan count is not correct.");

    ix_ScanIterator.close();
}

void testScanDelete() {
    RID rid;
    IXFileHandle ixFileHandle;
    IX_ScanIterator ix_ScanIterator;

    // open index file
    RC rc = indexManager.openFile(indexFileName, ixFileHandle);
    assert(rc == success && "indexManager::openFile() should not fail.");

    // scan - EXACT MATCH
    rc = indexManager.scan(ixFileHandle, testAttribute, NULL, NULL, true, true, ix_ScanIterator);
    assert(rc == success && "indexManager::scan() should not fail.");

    std::cout << IX_globals::activeIterators->size() << std::endl;

    // Delete entries in IndexScan Iterator
    unsigned count = 0; int key = 0;
    while (ix_ScanIterator.getNextEntry(rid, &key) == success) {
        std::cout << "Key: " << key << ", RID: " << rid.pageNum << rid.slotNum << std::endl;
        count++;

        rc = indexManager.deleteEntry(ixFileHandle, testAttribute, &key, rid);

        if (rc != IX_OK) {
            std::cout << "Count: " << count << ", RC: " << rc << std::endl;
        }
        assert(rc == success && "indexManager::deleteEntry() should not fail.");
    }
    std::cerr << std::endl;

    indexManager.printBtreeDebug(ixFileHandle,testAttribute);

    std::cerr << "testScanDelete( has passed)" << std::endl;
}

/* Scan and PrintBtree test */
int main() {
    testAttribute.type = TypeInt;
    testAttribute.name = "TestInt";
    testAttribute.length = 4;

    buildBtree();
    testScanMinusInfPlusInf();
    testScanloInclusive();
    testScanloExclusive();
    testScanloInclusiveEOP();
    testScanloExclusiveEOP();

    testScanHighInclusive();
    testScanHighExclusive();
    testScanBoth();

    buildBtree();
    testScanDelete();
}