//
// Created by Seoyeon Hwang on 10/25/19.
//

#include "rm_test_util.h"


int main(){

    RC rc = rm.createCatalog();
    if(rc != 0){
        std::cerr << "create catalog error" << std::endl;
        return 1;
    }


    vector<Attribute> testAttributes;
    Attribute attr;
    attr.name = "number";
    attr.type = TypeInt;
    attr.length = (AttrLength) 4;
    testAttributes.push_back(attr);

    attr.name = "name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength) 40;
    testAttributes.push_back(attr);

    attr.name = "height";
    attr.type = TypeReal;
    attr.length = (AttrLength) 4;
    testAttributes.push_back(attr);

    rc = rm.createTable("test", testAttributes);
    rc = rm.createTable("test1", testAttributes);
    rc = rm.createTable("test2", testAttributes);


    rc = rm.deleteCatalog();
    if(rc != 0){
        std::cerr << "delete catalog error" << std::endl;
        return 1;
    }

    remove("test.tbl");
    remove("test1.tbl");
    remove("test2.tbl");

    return 0;
}