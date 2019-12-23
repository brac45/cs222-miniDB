//
// Created by jong on 12/3/19.
// Test TableScan and IndexScan
//

#include "qe_test_util.h"

RC testCase_5() {
    TableScan heapScanIter(rm, "left");

    byte record[PAGE_SIZE];
    std::vector<Attribute> leftAttrs;
    std::vector<std::string> leftAttrsNames;
    leftAttrsNames.push_back("A");
    leftAttrsNames.push_back("C");
    heapScanIter.getAttributes(leftAttrs);

    Attribute indexedAttr;
    for (const Attribute& attr: leftAttrs) {
        if (attr.name == "left.C") {
            indexedAttr = attr;
            indexedAttr.name = "C";
        }
    }

    RM_ScanIterator rmsi;
    heapScanIter.setIterator();
    rm.scan("left", "", NO_OP, NULL, leftAttrsNames, rmsi);
    std::vector<Attribute> test;
    test.push_back(leftAttrs[0]);
    test.push_back(leftAttrs[2]);
    RID rid;
    while (rmsi.getNextTuple(rid, record) != RM_EOF) {
        std::cout << "(" << rid.pageNum << ", " << rid.slotNum << ") > ";
        rm.printTuple(test, record);
    }

    IndexManager &ix = IndexManager::instance();
    ix.printBtreeDebug("leftC.idx", indexedAttr);

    return success;
}

int main() {
    // Tables created: none
    // Indexes created: none

    if (testCase_5() != success) {
        cerr << "***** [FAIL] QE Test Case 97 failed. *****" << endl;
        return fail;
    } else {
        cerr << "***** QE Test Case 97 finished. The result will be examined. *****" << endl;
        return success;
    }
}
