//
// Created by jong on 10/6/19.
// For experiments
//

#include <cstring>
#include "pfm.h"

#define SIZE 16

int main() {
    byte* testptr = new byte[SIZE];

    for (unsigned i=0; i<SIZE; i++) {
        testptr[i] = i;
    }

    unsigned short temp = 8194;
    memcpy(testptr+(SIZE-2), &temp, 2);
    memcpy(testptr+(SIZE-4), &temp, 2);

    for (unsigned i=0; i<SIZE; i++) {
        std::cout << (unsigned)testptr[i] << ", ";
    }
    std::cout << std::endl;

    unsigned short test = 0;
    memcpy(&test, testptr+(SIZE-2), 2);
    std::cout << (unsigned int)test << std::endl;
    test = 0;
    memcpy(&test, testptr+(SIZE-4), 2);
    std::cout << (unsigned int)test << std::endl;

    std::cout << sizeof(testptr) << std::endl;

    delete[] testptr;

    int i = 0;
    for (; i<3; i++) {

    }

    std::cout << i << std::endl;

    return 0;
}
