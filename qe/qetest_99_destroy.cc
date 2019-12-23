//
// Created by Seoyeon Hwang on 11/24/19.
//
#include "qe_test_util.h"

int main(){

    // to destroy all catalog files and tbl, idx files made in qetest_01 and qetest_02
    RC rc = rm.destroyIndex("left", "B");
    if(rc!=0){ std::cout << "error occurred while destroying index file leftB" << std::endl; return 1;}

    rc = rm.destroyIndex("left", "C");
    if(rc!=0){ std::cout << "error occurred while destroying index file leftC" << std::endl; return 1;}

    rc = rm.destroyIndex("right", "B");
    if(rc!=0){ std::cout << "error occurred while destroying index file rightB" << std::endl; return 1;}

    rc = rm.destroyIndex("right", "C");
    if(rc!=0){ std::cout << "error occurred while destroying index file rightC" << std::endl; return 1;}

    rc = rm.deleteTable("left");
    if(rc!=0){ std::cout << "error occurred while deleting table left" << std::endl; return 1;}

    rc = rm.deleteTable("right");
    if(rc!=0){ std::cout << "error occurred while deleting table right" << std::endl; return 1;}

    rc = rm.deleteCatalog();
    if(rc!=0){ std::cout << "error occurred while deleting catalogs" << std::endl; return 1;}

    std::cout << "Destroy everything completed, check the files" << std::endl;
    return 0;
}