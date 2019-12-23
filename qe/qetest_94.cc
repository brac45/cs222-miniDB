//
// Created by Seoyeon Hwang on 12/6/19.
// To check the table left and right having valuees properly
//


#include "qe_test_util.h"


RC exitWithError(const TableScan *leftIn, const TableScan *rightIn, void *data) {
    delete leftIn;
    delete rightIn;
    free(data);
    return fail;
}

RC testCase_95() {
    // Mandatory for grad teams/solos
    // Optional for - undergrad solo (+5 extra credit points will be given based on the results of the BNLJ related tests)
    // 1. BNLJoin -- on TypeInt Attribute
    // SELECT * FROM left, right where left.B = right.B
    std::cerr << std::endl << "***** In QE Test Case 95 *****" << std::endl;

    RC rc = success;
    // Prepare the iterator and condition
    auto *leftIn = new TableScan(rm, "left");
    auto *rightIn = new TableScan(rm, "right");

    void *data = malloc(bufSize);
    bool nullBit;

    while (leftIn->getNextTuple(data) != QE_EOF) {
        int offset = 0;

        // Is an attribute left.A NULL?
        nullBit = *(unsigned char *) ((char *) data) & ((unsigned) 1 << (unsigned) 7);
        if (nullBit) {
            std::cerr << std::endl << "***** A returned value is not correct. *****" << std::endl;
            return exitWithError(leftIn, rightIn, data);
        }
        // Print left.A
        std::cerr << "left.A " << *(int *) ((char *) data + offset + 1) << std::endl;
        offset += sizeof(int);

        // Is an attribute left.B NULL?
        nullBit = *(unsigned char *) ((char *) data) & ((unsigned) 1 << (unsigned) 6);
        if (nullBit) {
            std::cerr << std::endl << "***** A returned value is not correct. *****" << std::endl;
            return exitWithError(leftIn, rightIn, data);
        }
        // Print left.B
        std::cerr << "left.B " << *(int *) ((char *) data + offset + 1) << std::endl;
        offset += sizeof(int);

        // Is an attribute left.C NULL?
        nullBit = *(unsigned char *) ((char *) data) & ((unsigned) 1 << (unsigned) 5);
        if (nullBit) {
            std::cerr << std::endl << "***** A returned value is not correct. *****" << std::endl;
            return exitWithError(leftIn, rightIn, data);
        }
        // Print left.C
        std::cerr << "left.C " << *(float *) ((char *) data + offset + 1) << std::endl;

    }

    while(rightIn->getNextTuple(data) != QE_EOF){
        int offset = 0;

        // Is an attribute right.B NULL?
        nullBit = *(unsigned char *) ((char *) data) & ((unsigned) 1 << (unsigned) 4);
        if (nullBit) {
            std::cerr << std::endl << "***** A returned value is not correct. *****" << std::endl;
            return exitWithError(leftIn, rightIn, data);
        }
        // Print right.B
        int valueB = *(int *) ((char *) data + offset + 1);
        std::cerr << "right.B " << valueB << std::endl;
        offset += sizeof(int);

        // Is an attribute right.C NULL?
        nullBit = *(unsigned char *) ((char *) data) & ((unsigned) 1 << (unsigned) 3);
        if (nullBit) {
            std::cerr << std::endl << "***** A returned value is not correct. *****" << std::endl;
            return exitWithError(leftIn, rightIn, data);
        }
        // Print right.C
        std::cerr << "right.C " << *(float *) ((char *) data + offset + 1) << std::endl;
        offset += sizeof(float);

        // Is an attribute right.D NULL?
        nullBit = *(unsigned char *) ((char *) data) & ((unsigned) 1 << (unsigned) 2);
        if (nullBit) {
            std::cerr << std::endl << "***** A returned value is not correct. *****" << std::endl;
            return exitWithError(leftIn, rightIn, data);
        }
        // Print right.D
        std::cerr << "right.D " << *(int *) ((char *) data + offset + 1) << std::endl;
    }


    delete leftIn;
    delete rightIn;
    free(data);
    return 0;
}

int main() {

    if (testCase_95() != success) {
        std::cerr << "***** [FAIL] QE Test Case 95 failed. *****" << std::endl;
        return fail;
    } else {
        std::cerr << "***** QE Test Case 95 finished. The result will be examined. *****" << std::endl;
        return success;
    }
}