//
// Created by jong on 12/3/19.
//

// For bit operations

#include "qe_test_util.h"

int main() {
    int test1_numOfAttrs = 20;
    int test2_numOfAttrs = 18;

    const int test1_nullSize = rbfm_helpers::getActualByteForNullsIndicator(test1_numOfAttrs);
    const int test2_nullSize = rbfm_helpers::getActualByteForNullsIndicator(test2_numOfAttrs);

    std::cout << "test1_nullSize: " << test1_nullSize << std::endl;
    std::cout << "test2_nullSize: " << test2_nullSize << std::endl;

    byte* test1 = new byte[test1_nullSize];
    byte* test2 = new byte[test2_nullSize];

    test1[0] = 255; test1[1] = 255;
    test2[0] = 255; test2[1] = 255;

    std::vector<bool> concat;

    int nullBitIter = 0;
    const byte MASK = 128;
    for (int i = 0; i < test1_numOfAttrs; ++i) {
        if ((i != 0) && (i % 8 == 0)) { nullBitIter++; }  // Advance null byte iterator if 8 bits are read

        const byte i_result = MASK & (byte)(test1[nullBitIter] << (unsigned)(i % 8));

        if (i_result == MASK) {
            concat.push_back(true);
        } else if (i_result == 0) {
            concat.push_back(false);
        } else {
            std::cerr << "Somethings wrong!" << std::endl;
        }
    }

    nullBitIter = 0;
    for (int i = 0; i < test2_numOfAttrs; ++i) {
        if ((i != 0) && (i % 8 == 0)) { nullBitIter++; }  // Advance null byte iterator if 8 bits are read

        const byte i_result = MASK & (byte)(test2[nullBitIter] << (unsigned)(i % 8));

        if (i_result == MASK) {
            concat.push_back(true);
        } else if (i_result == 0) {
            concat.push_back(false);
        } else {
            std::cerr << "Somethings wrong!" << std::endl;
        }
    }

    for (bool t: concat) {
        std::cout << t << "";
    }
    std::cout << std::endl;

    const int result_nullSize = rbfm_helpers::getActualByteForNullsIndicator(test1_numOfAttrs + test2_numOfAttrs);

    byte* result = new byte[result_nullSize];
    memset(result, 0, result_nullSize);     // Set to all zeroes for null-byte indicator

    nullBitIter = 0;
    for (int j = 0; j < test1_numOfAttrs + test2_numOfAttrs; ++j) {
        if ((j != 0) && (j % 8 == 0)) { nullBitIter++; }  // Advance null byte iterator if 8 bits are read
        const byte i_result = concat[j] ? MASK : 0;
        result[nullBitIter] = result[nullBitIter] | (byte)(i_result >> ((unsigned)j % 8));
    }

    for (int k = 0; k < result_nullSize; ++k) {
        std::bitset<8> printme(result[k]);
        std::cout << printme << "";
    }
    std::cout << std::endl;

    delete [] test1;
    delete [] test2;

    return success;
}
