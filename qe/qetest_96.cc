//
// Created by jong on 12/3/19.
// Print left and right tables from public test cases
//

#include "qe_test_util.h"

RC testCase_5() {
    TableScan leftHeapScan(rm, "left");
    TableScan rightHeapScan(rm, "right");

    byte buffer[PAGE_SIZE];

    std::cout << " ============= Left Table ================" << std::endl;
    std::vector<Attribute> attrs;
    leftHeapScan.getAttributes(attrs);
    while (leftHeapScan.getNextTuple(buffer) != QE_EOF) {
        rm.printTuple(attrs, buffer);
    }

    attrs.clear();
    std::cout << " ============= Right Table ================" << std::endl;
    rightHeapScan.getAttributes(attrs);
    while (rightHeapScan.getNextTuple(buffer) != QE_EOF) {
        rm.printTuple(attrs, buffer);
    }

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
