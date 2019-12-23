//
// Created by jong on 12/6/19.
//
#include "qe_test_util.h"

int main() {
    byte* tupleBuf[PAGE_SIZE]; RID rid;
    std::vector<Attribute> attrs;
    rm.getAttributes("Tables", attrs);

    // Get Attribute Names from RM
    std::vector<std::string> attrNames;
    for (Attribute &attr : attrs) {
        // convert to char *
        attrNames.push_back(attr.name);
    }
    RM_ScanIterator rmscan;
    rm.scan("Tables", "", NO_OP, NULL, attrNames, rmscan);

    while (rmscan.getNextTuple(rid, tupleBuf) != RM_EOF) {
        rm.printTuple(attrs, tupleBuf);
    }
    std::cout<<std::endl;
    std::cout<<std::endl;

    rmscan.close();
    attrs.clear(); attrNames.clear();

    rm.getAttributes("Columns", attrs);
    for (Attribute &attr : attrs) {
        // convert to char *
        attrNames.push_back(attr.name);
    }
    rm.scan("Columns", "", NO_OP, NULL, attrNames, rmscan);

    while (rmscan.getNextTuple(rid, tupleBuf) != RM_EOF) {
        rm.printTuple(attrs, tupleBuf);
    }

    rmscan.close();

    return 0;
}