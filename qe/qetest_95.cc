//
// Created by jong on 12/5/19.
//

#include "qe_test_util.h"


// To understand multimap
RC testCase_96() {
    std::unordered_multimap<int, byte*> testmap;

    // Insert 10 items in key 1
    for (int i = 0; i < 10; ++i) {
        byte* temp = new byte[4];
        *(int*)temp = i;
        std::pair<int, byte*> item(1, temp);
        testmap.insert(item);
    }

    // Insert 5 items in key 2
    for (int i = 20; i < 25; ++i) {
        byte* temp = new byte[4];
        *(int*)temp = i;
        std::pair<int, byte*> item(2, temp);
        testmap.insert(item);
    }

    // Insert 10 items in key 3
    for (int i = 30; i < 40; ++i) {
        byte* temp = new byte[4];
        *(int*)temp = i;
        std::pair<int, byte*> item(3, temp);
        testmap.insert(item);
    }

    auto it = testmap.equal_range(3);
    // Prints all the elements of key 1
    for (auto itr = it.first; itr != it.second; ++itr) {
        cout << itr->first
             << '\t' << *(int*)(itr->second) << '\n';
    }

    /* clean up */
    for (auto & itr : testmap){
        delete[] itr.second;
    }

    return success;
}

int main() {
    // Tables created: none
    // Indexes created: none

    if (testCase_96() != success) {
        cerr << "***** [FAIL] QE Test Case 97 failed. *****" << endl;
        return fail;
    } else {
        cerr << "***** QE Test Case 97 finished. The result will be examined. *****" << endl;
        return success;
    }
}
