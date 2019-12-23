//
// Created by jong on 11/12/19.
// RID operator overloading tests
//

#include "rbfm.h"
#include <cassert>

int main() {
    std::cout << "==== Testing rbftest_98 - Operator overloading =====";

    RID test1;
    test1.pageNum = 77;
    test1.slotNum = 88;

    RID test2;
    test2.pageNum = 77;
    test2.slotNum = 88;

    // Test equality operators
    assert((test1 == test2) && "Should be equal.");
    assert((test1 <= test2) && "Should be lte.");
    assert((test1 >= test2) && "Should be gte.");

    test2.slotNum = 100;

    assert((test1 < test2) && "Should be true.");
    assert(!(test1 > test2) && "Should be false.");

    test2.slotNum = 1;

    assert(!(test1 < test2) && "Should be true.");
    assert((test1 > test2) && "Should be false.");

    return 0;
}
