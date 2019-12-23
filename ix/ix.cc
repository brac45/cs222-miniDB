#include <cfloat>
#include "ix.h"

#define COMPARE_TYPE(a, b, type) (*(type*)a==*(type*)b ? 0: (*(type*)a>*(type*)b? 1 : -1))

////////////// ix_helpers Definitions ///////////////

RC ix_helpers::UpdateVariables(IXFileHandle &ixFileHandle){
    byte buffer[PAGE_SIZE] = { 0 }; unsigned offset = 0;

    memcpy(buffer + offset, &(ixFileHandle.ixReadPageCounter), 4); offset += 4;
    memcpy(buffer + offset, &(ixFileHandle.ixWritePageCounter), 4); offset += 4;
    memcpy(buffer + offset, &(ixFileHandle.ixAppendPageCounter), 4); offset += 4;
    memcpy(buffer + offset, &(ixFileHandle.rootPageNum), 4); offset += 4;
    memcpy(buffer + offset, &(ixFileHandle.init), 4);

    int seekResult = fseek(ixFileHandle.filePointer, 0, SEEK_SET);
    if (seekResult != 0){
        //std::cerr << "fseek error while update" << std::endl;
        return 1;
    }

    auto updateCounter = fwrite(buffer, sizeof(byte), PAGE_SIZE, ixFileHandle.filePointer);
    if (updateCounter != PAGE_SIZE) {
        //std::cerr << "fwrite error while update" << std::endl;
        return 1;
    }
    return 0;
}

RC ix_helpers::insertInit(IXFileHandle &ixFileHandle, const Attribute &attribute, const void* key, const RID& rid){

    /* To initialize, create a leaf node page and set it as a root */
    /* Leaf node has format : [ isLeaf | N | F | nextLeaf | (key_1|rid_1) | ... | (key_N|rid_N) ]*/

    PageNum nodePageNum = ixFileHandle.getNumberOfPages(); // should be 0

    byte newPage[PAGE_SIZE] = { 0 };
    RC rc = ixFileHandle.appendPage(newPage); //page 0 = root
    if(rc != 0) return 1;

    byte isLeaf = 1;
    unsigned N = 1;
    unsigned F = PAGE_SIZE - (1 + 4 + 4 + 4); // isLeaf(1), N(4), F(4), nextLeaf(4)
    int nextLeaf = -1; // There's only one root page right now
    unsigned offset = 0;

    void* newLeafPage = malloc(PAGE_SIZE);

    memcpy((byte*)newLeafPage + offset, &isLeaf, 1); offset += 1;
    memcpy((byte*)newLeafPage + offset, &N, 4); offset += 4;

    if(attribute.type == TypeInt || attribute.type == TypeReal){
        F -= (4 + 8); // key value(4), rid(pageNum|slotNum)(8)
        memcpy((byte*)newLeafPage + offset, &F, 4); offset += 4;
        memcpy((byte*)newLeafPage + offset, &nextLeaf, 4); offset += 4;
        memcpy((byte*)newLeafPage + offset, key, 4); offset += 4;
        memcpy((byte*)newLeafPage + offset, &rid.pageNum, 4); offset += 4;
        memcpy((byte*)newLeafPage + offset, &rid.slotNum, 4);
    }else if(attribute.type == TypeVarChar){
        unsigned length; memcpy(&length, key, 4);
        F -= (4 + length + 8); // key length(4), key value(length), rid(pageNum|slotNum)(8)
        memcpy((byte*)newLeafPage + offset, &F, 4); offset += 4;
        memcpy((byte*)newLeafPage + offset, &nextLeaf, 4); offset += 4;
        memcpy((byte*)newLeafPage + offset, key, 4 + length); offset += 4+length;
        memcpy((byte*)newLeafPage + offset, &rid.pageNum, 4); offset += 4;
        memcpy((byte*)newLeafPage + offset, &rid.slotNum, 4);
    }

    ixFileHandle.init = 0;
    ixFileHandle.rootPageNum = nodePageNum;

    // update init variable
    int updateResult = ix_helpers::UpdateVariables(ixFileHandle);
    if(updateResult != 0){
        free(newLeafPage);
        return 2;
    }

    // flush into disk
    rc = ixFileHandle.writePage(nodePageNum, newLeafPage);
    if(rc!=0){
        free(newLeafPage);
        return 1;
    }

    free(newLeafPage);
    return 0;

}

RC ix_helpers::insertTree(IXFileHandle &ixFileHandle, unsigned nodePageNum, const Attribute &attribute, const void* key, const RID& rid, void* &newChildEntry){

    void* currentPage = malloc(PAGE_SIZE);
    RC rc = ixFileHandle.readPage(nodePageNum, currentPage);
    if(rc != 0){
        free(currentPage);
        return 1;
    }

    byte isLeaf=0; unsigned N=0, F=0, lessThan=0, greaterThenEqualTo=0, offset=0, currentKeyLength=0, keyLength=0;
    int nextLeaf = -1;

    void* currentKey = malloc(PAGE_SIZE); // key value (without length) would be less than one page
    //RID currentRID;

    memcpy(&isLeaf, currentPage, 1); offset = 1;
    memcpy(&N, (byte*)currentPage + offset, 4); offset += 4;
    memcpy(&F, (byte*)currentPage+ offset, 4); offset += 4;

    RC cmpResult;

    if(isLeaf == 0){

        /* Find i such that K_i <= key < K_(i+1) */
        /* Read [ lessThan | K_i | greaterThanEqualTo ] for each i=0,...,N-1 */

        unsigned offsetToInsert=0;
        rc = ix_helpers::FindWhereToInsertInter(currentPage, attribute, key, rid, offsetToInsert);
        if(rc != 0){
            std::cerr << "FindWhereToInsertInter error" << std::endl;
            free(currentKey);
            free(currentPage);
            return 1;
        }

        unsigned pageNumToGo=0;
        memcpy(&pageNumToGo, (byte*)currentPage + offsetToInsert - 4, 4);

        insertTree(ixFileHandle, pageNumToGo, attribute, key, rid, newChildEntry);

        //printf("newChildEntry: %p\n", newChildEntry);

        if(newChildEntry == nullptr){

            free(currentKey);
            free(currentPage);
            return 0;

        }else{ // need to split child => must insert newChildEntry in current node

            unsigned needSpaceAmount = 12; // for rid and pageNum in newChildEntry:[ (key_value|rid) | pageNum ]
            if(attribute.type == TypeInt || attribute.type == TypeReal){
                needSpaceAmount += 4;
            }else if(attribute.type == TypeVarChar){
                unsigned childLength;
                memcpy(&childLength,  newChildEntry, 4);
                needSpaceAmount += 4+childLength;
            }

            if(F >= needSpaceAmount){ // put newChildEntry in this node

                //shift the right keys and ptrs to the right and write the newChildEntry between K_i and K_i+1

                rc = ix_helpers::FindWhereToInsertInter(currentPage, attribute, key, rid, offsetToInsert);
                if(rc!=0){
                    free(currentKey);
                    free(currentPage);
                    return 1;
                }
                rc = ix_helpers::shiftToRightandInsertChild(currentPage, attribute, offsetToInsert, needSpaceAmount, newChildEntry);
                if(rc!=0){
                    free(currentKey);
                    free(currentPage);
                    return 1;
                }

                newChildEntry = nullptr;

                /* flush into the file*/
                rc = ixFileHandle.writePage(nodePageNum, currentPage);
                if(rc!= 0){
                    free(currentKey);
                    free(currentPage);
                    return 1;
                }

                free(currentKey);
                free(currentPage);
                return 0;

            }else{ // need to split current node and insert newChildEntry in proper position

                void* newNode = malloc(PAGE_SIZE);
                void* newParentKey = malloc(PAGE_SIZE);
                rc = ix_helpers::splitNodeIntoTwoInter(ixFileHandle, currentPage, attribute, newNode, newParentKey);
                if(rc != 0){
                    std::cerr << "splitNodeIntoTwoInter error" << std::endl;
                    free(currentKey);
                    free(currentPage);
                    free(newNode);
                    free(newParentKey);
                    return 1;
                }

                /* After split, newParentKey = [key_i | rid_i]  such that currentPage's keys < key_i <= newNode's keys
                 * Set this key as the newChildEntry with pageNum of current node and pageNum of new node */


                /* Derive the smallest key of new node to determine where to insert */

                void* SecondHalfStartingKey = malloc(PAGE_SIZE); //  smallest key of new node (without length if it's TypeVarChar)
                unsigned SecondHalfStartingKeyLength = 0; // used only when TypeVarChar
                RID SecondHalfStartingRID;
                unsigned newOffset = 13; // isLeaf(1), N(4), F(4), P0(4)

                if(attribute.type == TypeInt) {
                    memcpy(SecondHalfStartingKey, (byte*)newNode + newOffset, 4); newOffset += 4;
                    cmpResult = COMPARE_TYPE(key,SecondHalfStartingKey,int);
                    //cmpResult = memcmp(key, SecondHalfStartingKey, 4);
                }else if(attribute.type == TypeReal){
                    memcpy(SecondHalfStartingKey, (byte*)newNode + newOffset, 4); newOffset += 4;
                    cmpResult = COMPARE_TYPE(key,SecondHalfStartingKey,float);
                    //cmpResult = memcmp(key, SecondHalfStartingKey, 4);
                }else if(attribute.type == TypeVarChar){
                    memcpy(&SecondHalfStartingKeyLength, (byte*)newNode + newOffset, 4); newOffset += 4;
                    memcpy(SecondHalfStartingKey, (byte*)newNode + newOffset, SecondHalfStartingKeyLength); newOffset += SecondHalfStartingKeyLength;
                    memcpy(&keyLength, key, 4);
                    cmpResult = memcmp((byte*)key + 4, (byte*)SecondHalfStartingKey, std::min(keyLength, SecondHalfStartingKeyLength));
                }

                memcpy(&(SecondHalfStartingRID.pageNum), (byte*)newNode + newOffset, 4); newOffset += 4;
                memcpy(&(SecondHalfStartingRID.slotNum), (byte*)newNode + newOffset, 4); newOffset += 4;


                /* Insert newChildEntry into either current node or new node */

                if(cmpResult < 0){ // insert newChildEntry into current node
                    rc = ix_helpers::FindWhereToInsertInter(currentPage, attribute, key, rid, offsetToInsert);
                    if(rc!=0){
                        free(currentKey);
                        free(currentPage);
                        free(newNode);
                        free(newParentKey);
                        free(SecondHalfStartingKey);
                        return 1;
                    }
                    rc = ix_helpers::shiftToRightandInsertChild(currentPage, attribute, offsetToInsert, needSpaceAmount, newChildEntry);
                    if(rc!=0){
                        free(currentKey);
                        free(currentPage);
                        free(newNode);
                        free(newParentKey);
                        free(SecondHalfStartingKey);
                        return 1;
                    }
                }else if(cmpResult == 0){
                    if(rid < SecondHalfStartingRID){ // insert newChildEntry into current node
                        rc = ix_helpers::FindWhereToInsertInter(currentPage, attribute, key, rid, offsetToInsert);
                        if(rc!=0){
                            free(currentKey);
                            free(currentPage);
                            free(newNode);
                            free(newParentKey);
                            free(SecondHalfStartingKey);
                            return 1;
                        }
                        rc = ix_helpers::shiftToRightandInsertChild(currentPage, attribute, offsetToInsert, needSpaceAmount, newChildEntry);
                        if(rc!=0){
                            free(currentKey);
                            free(currentPage);
                            free(newNode);
                            free(newParentKey);
                            free(SecondHalfStartingKey);
                            return 1;
                        }
                    }else{ // insert newChildEntry into new node
                        rc = ix_helpers::FindWhereToInsertInter(newNode, attribute, key, rid, offsetToInsert);
                        if(rc!=0){
                            free(currentKey);
                            free(currentPage);
                            free(newNode);
                            free(newParentKey);
                            free(SecondHalfStartingKey);
                            return 1;
                        }
                        rc = ix_helpers::shiftToRightandInsertChild(newNode, attribute, offsetToInsert, needSpaceAmount, newChildEntry);
                        if(rc!=0){
                            free(currentKey);
                            free(currentPage);
                            free(newNode);
                            free(newParentKey);
                            free(SecondHalfStartingKey);
                            return 1;
                        }
                    }

                }else{ // insert newChildEntry into new node
                    rc = ix_helpers::FindWhereToInsertInter(newNode, attribute, key, rid, offsetToInsert);
                    if(rc!=0){
                        free(currentKey);
                        free(currentPage);
                        free(newNode);
                        free(newParentKey);
                        free(SecondHalfStartingKey);
                        return 1;
                    }
                    rc = ix_helpers::shiftToRightandInsertChild(newNode, attribute, offsetToInsert, needSpaceAmount, newChildEntry);
                    if(rc!=0){
                        free(currentKey);
                        free(currentPage);
                        free(newNode);
                        free(newParentKey);
                        free(SecondHalfStartingKey);
                        return 1;
                    }
                }

                /* Flush two nodes into disk */
                rc = ixFileHandle.appendPage(newNode);
                if(rc != 0) {
                    free(currentKey);
                    free(currentPage);
                    free(newNode);
                    free(newParentKey);
                    free(SecondHalfStartingKey);
                    return 1;
                }
                unsigned newPageNum = ixFileHandle.getNumberOfPages() - 1;
                rc = ixFileHandle.writePage(nodePageNum, currentPage);
                if(rc != 0) {
                    free(currentKey);
                    free(currentPage);
                    free(newNode);
                    free(newParentKey);
                    free(SecondHalfStartingKey);
                    return 1;
                }

                /* Set up newChildEntry as the newParentKey = [key|rid] and pageNum of the new node */
                newOffset = 0; unsigned len = 0;
                if(attribute.type == TypeInt || attribute.type == TypeReal) {
                    memcpy((byte*)newChildEntry + newOffset, (byte*)newParentKey + newOffset, 4); newOffset += 4;

                }else if(attribute.type == TypeVarChar) {
                    memcpy(&len, (byte*)newParentKey + newOffset, 4);
                    memcpy((byte *) newChildEntry + newOffset, (byte*)newParentKey + newOffset, 4); newOffset += 4;
                    memcpy((byte *) newChildEntry + newOffset, (byte*)newParentKey + newOffset, len); newOffset += len;
                }

                memcpy((byte *) newChildEntry + newOffset, (byte*)newParentKey + newOffset, 4); newOffset += 4; // for rid.pageNum
                memcpy((byte *) newChildEntry + newOffset, (byte*)newParentKey + newOffset, 4); newOffset += 4; // for rid.slotNum
                memcpy((byte *) newChildEntry + newOffset, &newPageNum, 4); newOffset += 4;

                /*
                 * If current node is root */
                if(nodePageNum == ixFileHandle.rootPageNum){
                    byte newRootPage[PAGE_SIZE] = {0}; offset = 0;
                    isLeaf = 0; N = 1; F = PAGE_SIZE - 9 - 4 - newOffset; // isLeaf, N, F, PageNum for current node, and newChildEntry
                    memcpy(newRootPage + offset, &isLeaf, 1); offset += 1;
                    memcpy(newRootPage + offset, &N, 4); offset += 4;
                    memcpy(newRootPage + offset, &F, 4); offset += 4;
                    memcpy(newRootPage + offset, &nodePageNum, 4); offset += 4;
                    memcpy(newRootPage + offset, newChildEntry, newOffset); offset += newOffset;

                    ixFileHandle.appendPage(newRootPage);
                    ixFileHandle.rootPageNum = ixFileHandle.getNumberOfPages() - 1;
                    ix_helpers::UpdateVariables(ixFileHandle);
                }

                free(currentKey);
                free(currentPage);
                free(newNode);
                free(newParentKey);
                free(SecondHalfStartingKey);
                return 0;

            }
        }


    }
    else{ //isLeaf == 1, i.e. leaf node

        memcpy(&nextLeaf, (byte*)currentPage + offset, 4); offset += 4;
        unsigned keyValueLen = 0, offsetToInsert=0;

        /* Calculate the length of inserting key value */
        unsigned needSpaceAmount = 0; // KeyBlock = [key|rid]
        if(attribute.type == TypeInt || attribute.type == TypeReal){
            needSpaceAmount = 4 + 8;
        }else if(attribute.type == TypeVarChar){
            memcpy(&keyValueLen, key, 4);
            needSpaceAmount = 4 + keyValueLen + 8;
        }

        if(F >= needSpaceAmount){

            rc = ix_helpers::FindWhereToInsertLeaf(currentPage, attribute, key, rid, offsetToInsert);
            if(rc!=0){
                free(currentKey);
                free(currentPage);
                return 1;
            }
            rc = ix_helpers::shiftToRightandInsertKey(currentPage, attribute, offsetToInsert, needSpaceAmount, key, rid);
            if(rc!=0){
                free(currentKey);
                free(currentPage);
                return 1;
            }
            newChildEntry = nullptr;

            /* flush into the file*/
            rc = ixFileHandle.writePage(nodePageNum, currentPage);
            if(rc!= 0){
                free(currentKey);
                free(currentPage);
                return 1;
            }

            free(currentKey);
            free(currentPage);
            return 0;

        }
        else{ // need to split current node

            /* split the node into two leaf nodes and set  sibling pointers (nextLeaf in the page format) */

            void* newNode = malloc(PAGE_SIZE);
            unsigned newPageNum=0;
            rc = ix_helpers::splitNodeIntoTwoLeaf(ixFileHandle, currentPage, attribute, newNode, newPageNum);
            if(rc != 0){
                std::cerr << "splitNodeIntoTwoLeaf error" << std::endl;
                free(currentKey);
                free(currentPage);
                free(newNode);
                return 1;
            }

            /* Derive the smallest key of new node to determine where to insert */

            void* SecondHalfStartingKey = malloc(PAGE_SIZE); //  smallest key of new node (without length if it's TypeVarChar)
            unsigned SecondHalfStartingKeyLength = 0; // used only when TypeVarChar
            RID SecondHalfStartingRID;
            unsigned newOffset = 13; // isLeaf(1), N(4), F(4), nextLeaf(4)

            if(attribute.type == TypeInt) {
                memcpy(SecondHalfStartingKey, (byte*)newNode + newOffset, 4); newOffset += 4;
                cmpResult = COMPARE_TYPE(key,SecondHalfStartingKey,int);
                //cmpResult = memcmp(key, SecondHalfStartingKey, 4);
            }else if(attribute.type == TypeReal) {
                memcpy(SecondHalfStartingKey, (byte*)newNode + newOffset, 4); newOffset += 4;
                cmpResult = COMPARE_TYPE(key,SecondHalfStartingKey,float);
            }else if(attribute.type == TypeVarChar){
                memcpy(&SecondHalfStartingKeyLength, (byte*)newNode + newOffset, 4); newOffset += 4;
                memcpy(SecondHalfStartingKey, (byte*)newNode + newOffset, SecondHalfStartingKeyLength); newOffset += SecondHalfStartingKeyLength;
                memcpy(&keyLength, key, 4);
                cmpResult = memcmp((byte*)key + 4, (byte*)SecondHalfStartingKey, std::min(keyLength, SecondHalfStartingKeyLength));
            }

            memcpy(&(SecondHalfStartingRID.pageNum), (byte*)newNode + newOffset, 4); newOffset += 4;
            memcpy(&(SecondHalfStartingRID.slotNum), (byte*)newNode + newOffset, 4); newOffset += 4;

            /* Insert KEY=(key,rid) into either current node or new node */

            if(cmpResult < 0){ // insert KEY into current node
                rc = ix_helpers::FindWhereToInsertLeaf(currentPage, attribute, key, rid, offsetToInsert);
                if(rc!=0) {
                    free(currentKey);
                    free(currentPage);
                    free(newNode);
                    free(SecondHalfStartingKey);
                    return 1;
                }
                rc = ix_helpers::shiftToRightandInsertKey(currentPage, attribute, offsetToInsert, needSpaceAmount, key, rid);
                if(rc!=0){
                    free(currentKey);
                    free(currentPage);
                    free(newNode);
                    free(SecondHalfStartingKey);
                    return 1;
                }
            }else if(cmpResult == 0){
                if(rid < SecondHalfStartingRID){ // insert KEY into current node
                    rc = ix_helpers::FindWhereToInsertLeaf(currentPage, attribute, key, rid, offsetToInsert);
                    if(rc!=0) {
                        free(currentKey);
                        free(currentPage);
                        free(newNode);
                        free(SecondHalfStartingKey);
                        return 1;
                    }
                    rc = ix_helpers::shiftToRightandInsertKey(currentPage, attribute, offsetToInsert, needSpaceAmount, key, rid);
                    if(rc!=0) {
                        free(currentKey);
                        free(currentPage);
                        free(newNode);
                        free(SecondHalfStartingKey);
                        return 1;
                    }
                }else{ // insert KEY into new node
                    rc = ix_helpers::FindWhereToInsertLeaf(newNode, attribute, key, rid, offsetToInsert);
                    if(rc!=0){
                        free(currentKey);
                        free(currentPage);
                        free(newNode);
                        free(SecondHalfStartingKey);
                        return 1;
                    }
                    rc = ix_helpers::shiftToRightandInsertKey(newNode, attribute, offsetToInsert, needSpaceAmount, key, rid);
                    if(rc!=0){
                        free(currentKey);
                        free(currentPage);
                        free(newNode);
                        free(SecondHalfStartingKey);
                        return 1;
                    }
                }

            }else{ // insert KEY into new node
                rc = ix_helpers::FindWhereToInsertLeaf(newNode, attribute, key, rid, offsetToInsert);
                if(rc!=0) {
                    free(currentKey);
                    free(currentPage);
                    free(newNode);
                    free(SecondHalfStartingKey);
                    return 1;
                }
                rc = ix_helpers::shiftToRightandInsertKey(newNode, attribute, offsetToInsert, needSpaceAmount, key, rid);
                if(rc!=0){
                    free(currentKey);
                    free(currentPage);
                    free(newNode);
                    free(SecondHalfStartingKey);
                    return 1;
                }
            }

            /* Flush two nodes into disk */
            rc = ixFileHandle.writePage(newPageNum, newNode);
            if(rc != 0){
                free(currentKey);
                free(currentPage);
                free(newNode);
                free(SecondHalfStartingKey);
                return 1;
            }

            rc = ixFileHandle.writePage(nodePageNum, currentPage);
            if(rc != 0){
                free(currentKey);
                free(currentPage);
                free(newNode);
                free(SecondHalfStartingKey);
                return 1;
            }

            /* Set the newChildEntry with the smallest key and pageNum of the new node */
            newOffset = 0; // for calculating newChildEntry length
            if(attribute.type == TypeInt || attribute.type == TypeReal){
                memcpy((byte*)newChildEntry, SecondHalfStartingKey, 4); //key value
                memcpy((byte*)newChildEntry + 4, &(SecondHalfStartingRID.pageNum), 4); //rid.pageNum
                memcpy((byte*)newChildEntry + 8, &(SecondHalfStartingRID.slotNum), 4); //rid.slotNum
                memcpy((byte*)newChildEntry + 12, &newPageNum, 4); //pageNum of newNode
                newOffset = 16;
            }else if(attribute.type == TypeVarChar){
                memcpy((byte*)newChildEntry, &SecondHalfStartingKeyLength, 4); // key length
                memcpy((byte*)newChildEntry + 4, SecondHalfStartingKey, SecondHalfStartingKeyLength); // key value
                memcpy((byte*)newChildEntry + 4 + SecondHalfStartingKeyLength, &(SecondHalfStartingRID.pageNum), 4); //rid.pageNum
                memcpy((byte*)newChildEntry + 8 + SecondHalfStartingKeyLength, &(SecondHalfStartingRID.slotNum), 4); //rid.slotNum
                memcpy((byte*)newChildEntry + 12 + SecondHalfStartingKeyLength, &newPageNum, 4); //pageNum of newNode
                newOffset = 16 + SecondHalfStartingKeyLength;
            }


            /* If current node is root */
            if(nodePageNum == ixFileHandle.rootPageNum){
                byte newRootPage[PAGE_SIZE] = {0}; offset = 0;
                isLeaf = 0; N = 1; F = PAGE_SIZE - 9 - 4 - newOffset; // isLeaf, N, F, PageNum for current node, and newChildEntry
                memcpy(newRootPage + offset, &isLeaf, 1); offset += 1;
                memcpy(newRootPage + offset, &N, 4); offset += 4;
                memcpy(newRootPage + offset, &F, 4); offset += 4;
                memcpy(newRootPage + offset, &nodePageNum, 4); offset += 4;
                memcpy(newRootPage + offset, newChildEntry, newOffset); offset += newOffset;

                ixFileHandle.appendPage(newRootPage);
                ixFileHandle.rootPageNum = ixFileHandle.getNumberOfPages() - 1;
                ix_helpers::UpdateVariables(ixFileHandle);
            }

            free(currentKey);
            free(currentPage);
            free(newNode);
            free(SecondHalfStartingKey);
            return 0;
        }

    }

}


RC ix_helpers::FindWhereToInsertInter(void* currentPage, const Attribute &attribute, const void* key, const RID& rid, unsigned& offsetToInsert){
// for intermediate node

    byte isLeaf=0; unsigned N=0,F=0, offset = 0, tmpStartingOffset=0, lessThan=0, greaterThenEqualTo=0, keyLength=0;

    memcpy(&isLeaf, (byte*)currentPage + offset, 1); offset += 1;
    if(isLeaf == 1){ //input node is not intermediate, return error
        std::cerr << "This is for intermediate node" << std::endl;
        return 1;
    }
    memcpy(&N, (byte*)currentPage + offset, 4); offset += 4;
    memcpy(&F, (byte*)currentPage+ offset, 4); offset += 4;

    if(attribute.type == TypeVarChar){
        memcpy(&keyLength, key, 4);
    }

    void* currentKey = malloc(PAGE_SIZE); // key value (without length) would be less than one page
    unsigned currentKeyLength=0; RID currentRID;
    RC cmpResult=0;

    /* FIXME: Jong) added condition where N == 0, if so, we must set offset to start of the leaf node (after headers) */
    offsetToInsert = 13;

    for(unsigned i = 0; i < N ; i++){

        tmpStartingOffset = offset;

        memcpy(&lessThan,  (byte*)currentPage + offset, 4); offset += 4;

        if(attribute.type == TypeInt){
            memcpy(currentKey, (byte*)currentPage + offset, 4); offset += 4;
            cmpResult = COMPARE_TYPE(key, currentKey, int);
            //cmpResult = memcmp(key, currentKey, 4);
        }else if(attribute.type == TypeReal){
            memcpy(currentKey, (byte*)currentPage + offset, 4); offset += 4;
            cmpResult = COMPARE_TYPE(key, currentKey, float);
        }else if(attribute.type == TypeVarChar) {
            memcpy(&currentKeyLength, (byte *)currentPage + offset, 4); offset += 4;
            memcpy(currentKey, (byte *)currentPage + offset, currentKeyLength); offset += currentKeyLength;
            cmpResult = memcmp((byte *) key + 4, currentKey, std::min(currentKeyLength, keyLength));
        }

        memcpy(&currentRID.pageNum, (byte*)currentPage + offset, 4); offset += 4;
        memcpy(&currentRID.slotNum, (byte*)currentPage + offset, 4); offset += 4;
        memcpy(&greaterThenEqualTo,  (byte*)currentPage  + offset, 4);

        /* Assign the insertion position into offsetToInsert, which is [...|P_i-1 | K_i | P_i | (*here*) K_i+1 | P_i+1|...] where K_i <= [input key value] < K_(i+1) */
        if(cmpResult < 0){ // key value < current key value

            offsetToInsert = tmpStartingOffset + 4; // to insert [ lessThan | (*here*) currentKey | greaterThanEqualTo ]
            break;

        }else if(cmpResult == 0){ // key value == current key value

            if(rid < currentRID){ // rid < currentRID
                offsetToInsert = tmpStartingOffset + 4; // to insert [ lessThan | (*here*) currentKey | greaterThanEqualTo ]
                break;
            }else{ // rid >= currentRID
                if(i == N-1){ // if this is the last key, we should see inside of the last ptr
                    offsetToInsert = offset + 4; // Since current offset is [ lessThan | currentKey | (*here*) greaterThanEqualTo ] and we need to insert [ lessThan | currentKey | greaterThanEqualTo (*here*) ]
                    break;
                }
                continue;
            }

        }else{ // key value > current key value

            if(i == N-1){ // if this is the last key, we should see inside of the last ptr
                offsetToInsert = offset + 4; // Since current offset is [ lessThan | currentKey | (*here*) greaterThanEqualTo ] and we need to insert [ lessThan | currentKey | greaterThanEqualTo (*here*) ]
                break;
            }
            continue;

        }
    }

    free(currentKey);
    return 0;

}

RC ix_helpers::FindWhereToInsertLeaf(void* currentPage, const Attribute &attribute, const void* key, const RID& rid, unsigned& offsetToInsert){
//for leaf node

    byte isLeaf=0; unsigned N=0,F=0, offset = 0, tmpStartingOffset=0, keyLength=0;
    int nextLeaf=0;

    memcpy(&isLeaf, (byte*)currentPage + offset, 1); offset += 1;
    if(isLeaf == 0){ //input node is not leaf, return error
        std::cerr << "This is for leaf node" << std::endl;
        return 1;
    }
    memcpy(&N, (byte*)currentPage + offset, 4); offset += 4;
    memcpy(&F, (byte*)currentPage+ offset, 4); offset += 4;
    memcpy(&nextLeaf, (byte*)currentPage+ offset, 4); offset += 4;

    if(attribute.type == TypeVarChar){
        memcpy(&keyLength, key, 4);
    }

    void* currentKey = malloc(PAGE_SIZE); // key value (without length) would be less than one page
    unsigned currentKeyLength=0; RID currentRID;
    RC cmpResult=0;

    /* FIXME: Jong) added condition where N == 0, if so, we must set offset to start of the leaf node (after headers) */
    offsetToInsert = 13;

    for(int i = 0; i < N ; i++){

        tmpStartingOffset = offset;

        if(attribute.type == TypeInt){
            memcpy(currentKey, (byte*)currentPage + offset, 4); offset += 4;
            cmpResult = COMPARE_TYPE(key, currentKey, int);
            //cmpResult = memcmp(key, currentKey, 4);
        }else if(attribute.type == TypeReal){
            memcpy(currentKey, (byte*)currentPage + offset, 4); offset += 4;
            cmpResult = COMPARE_TYPE(key, currentKey, float);
        }else if(attribute.type == TypeVarChar) {
            memcpy(&currentKeyLength, (byte *)currentPage + offset, 4); offset += 4;
            memcpy(currentKey, (byte *)currentPage + offset, currentKeyLength); offset += currentKeyLength;
            cmpResult = memcmp((byte *) key + 4, currentKey, std::min(currentKeyLength, keyLength));
        }

        memcpy(&currentRID.pageNum, (byte*)currentPage + offset, 4); offset += 4;
        memcpy(&currentRID.slotNum, (byte*)currentPage + offset, 4); offset += 4;

        /* Assign the insertion position into offsetToInsert, which is [...|K_i | (*here*) K_i+1 |...] where K_i <= [input key value] < K_(i+1) */
        if(cmpResult < 0){ // key value < current key value

            offsetToInsert = tmpStartingOffset; //to insert [ ... | (*here*) currentKey,currentRID | ... ]
            break;

        }else if(cmpResult == 0){ // key value == current key value

            if(rid < currentRID){ // rid < currentRID
                offsetToInsert = tmpStartingOffset; // to insert [ ... | (*here*) currentKey,currentRID | ... ]
                break;
            }else{ // rid >= currentRID
                if(i == N-1){ // if this is the last key, we should see inside of the last ptr
                    offsetToInsert = offset; // to insert [ ... | currentKey,currentRID (*here*) ]
                    break;
                }
                continue;
            }

        }else{ // key value > current key value

            if(i == N-1){ // if this is the last key, we should see inside of the last ptr
                offsetToInsert = offset; // to insert [ ... | currentKey,currentRID (*here*) ]
                break;
            }
            continue;

        }
    }

    free(currentKey);
    return 0;

}


RC ix_helpers::shiftToRightandInsertChild(void* currentPage, const Attribute &attribute, const unsigned offsetToInsert, const unsigned needSpaceAmount, const void* data){
// only for intermediate node: [ isLeaf | N | F | P0 | K1 | P1 | ... | P_N-1 | K_N | P_N ]
// *data is like: [ KEY | PageNum ] (newChildEntry)

    unsigned N=0, F=0, shiftBlockLength=0, source=0, destination=0, offset = 0;
    memcpy(&N, (byte*)currentPage+ 1, 4);
    memcpy(&F, (byte*)currentPage+ 5, 4);

    shiftBlockLength = (PAGE_SIZE - F) - offsetToInsert;
    source = offsetToInsert;
    destination = source + needSpaceAmount;

    // shift the block to the right
    memmove((byte*)currentPage + destination, (byte*)currentPage + source, shiftBlockLength);

    // insert data to that position
    if(attribute.type == TypeInt || attribute.type == TypeReal ){
        memcpy((byte*)currentPage + source + offset, (byte*)data + offset, 4); offset += 4;
    }else if(attribute.type == TypeVarChar){
        unsigned len=0;
        memcpy(&len, data, 4);
        memcpy((byte*)currentPage + source + offset, (byte*)data + offset, 4); offset += 4;
        memcpy((byte*)currentPage + source + offset, (byte*)data + offset, len); offset += len;
    }

    memcpy((byte*)currentPage + source + offset, (byte*)data + offset, 4); offset += 4; //for rid.pageNum
    memcpy((byte*)currentPage + source + offset, (byte*)data + offset, 4); offset += 4; //for rid.slotNum
    memcpy((byte*)currentPage + source + offset, (byte*)data + offset, 4); offset += 4; //for PageNum (ptr)

    // update N, F
    N += 1; F -= needSpaceAmount;
    memcpy((byte*)currentPage+ 1, &N, 4);
    memcpy((byte*)currentPage+ 5, &F, 4);

    return 0;

}

RC ix_helpers::shiftToRightandInsertKey(void* currentPage, const Attribute &attribute, const unsigned offsetToInsert, const unsigned needSpaceAmount, const void* key, const RID &rid){
// only for leaf node: [ isLeaf | N | F | nextLeaf | K1 | K2 | ... | K_N ]
// need to insert KEY = [key|rid]

    unsigned N=0, F=0, shiftBlockLength=0, source=0, destination=0, offset = 0;
    memcpy(&N, (byte*)currentPage+ 1, 4);
    memcpy(&F, (byte*)currentPage+ 5, 4);

    shiftBlockLength = (PAGE_SIZE - F) - offsetToInsert;
    source = offsetToInsert;
    destination = source + needSpaceAmount;

    // shift the block to the right
    memmove((byte*)currentPage + destination, (byte*)currentPage + source, shiftBlockLength);

    // insert data to that position
    if(attribute.type == TypeInt || attribute.type == TypeReal ){
        memcpy((byte*)currentPage + source, (byte*)key, 4); offset += 4;
    }else if(attribute.type == TypeVarChar){
        unsigned len=0;
        memcpy(&len, key, 4);
        memcpy((byte*)currentPage + source, (byte*)key, 4); offset += 4;
        memcpy((byte*)currentPage + source + 4, (byte*)key + 4, len); offset += len;
    }

    memcpy((byte*)currentPage + source + offset, &(rid.pageNum), 4); offset += 4; //for rid.pageNum
    memcpy((byte*)currentPage + source + offset, &(rid.slotNum), 4); offset += 4; //for rid.slotNum

    // update N, F
    N += 1; F -= needSpaceAmount;
    memcpy((byte*)currentPage+ 1, &N, 4);
    memcpy((byte*)currentPage+ 5, &F, 4);

    return 0;

}

RC ix_helpers::splitNodeIntoTwoInter(IXFileHandle &ixFileHandle, void *currentPage, const Attribute &attribute, void *newNode, void *SecondHalfStartingKey) {
// only for intermediate node

    byte isLeaf=0; unsigned N=0, F=0, newN=0, newF=0, offset=0, newOffset=0, FirstHalfEnd=0, SecondHalfStart=0, tmpStartingOffset = 0; // lengthOfSecondStartingKey = 0;

    memcpy(&isLeaf, (byte*)currentPage + offset, 1); offset += 1;
    if(isLeaf == 1){ //input node is not intermediate, return error
        std::cerr << "This is for intermediate node" << std::endl;
        return 1;
    }
    memcpy(&N, (byte*)currentPage + offset, 4); offset += 4;
    memcpy(&F, (byte*)currentPage+ offset, 4); offset += 4;
    offset += 4; // to skip P_0

    for(int i=0 ; i < N ; i++){
        // [ K_i(key,rid) | P_i ]

        unsigned keyOffset = 0;
        tmpStartingOffset = offset;

        if(attribute.type == TypeInt || attribute.type == TypeReal){
            memcpy((byte*)SecondHalfStartingKey + keyOffset, (byte*)currentPage + offset, 4);
            offset += 4; keyOffset += 4;
        }else if(attribute.type == TypeVarChar) {
            unsigned  currentKeyLength=0;
            memcpy(&currentKeyLength, (byte *)currentPage + offset, 4);
            memcpy((byte*) SecondHalfStartingKey + keyOffset, (byte*)currentPage + offset, 4);
            offset += 4; keyOffset += 4;
            memcpy((byte*)SecondHalfStartingKey + keyOffset, (byte*)currentPage + offset, currentKeyLength);
            offset += currentKeyLength; keyOffset += currentKeyLength;
        }

        memcpy((byte*)SecondHalfStartingKey + keyOffset, (byte*)currentPage + offset, 4); //for rid.pageNum
        offset += 4; keyOffset += 4;
        memcpy((byte*)SecondHalfStartingKey + keyOffset, (byte*)currentPage + offset, 4); //for rid.slotNum
        offset += 4; keyOffset += 4;

        offset += 4; // for P_i

        if( offset > (PAGE_SIZE/2) ) {
            FirstHalfEnd = tmpStartingOffset;
            SecondHalfStart = offset - 4; // to include P_i   TODO: Before: jong
            //lengthOfSecondStartingKey = keyOffset;
            newN = N - (i + 1); N = i;
            break;
        }
    }

    unsigned SecondHalfLength = (PAGE_SIZE - F) - SecondHalfStart; // to include the last pageNum (P_N)
    newF = PAGE_SIZE - 9 - SecondHalfLength;
    F = FirstHalfEnd; // to include P_i

    /* Format newNode and move the second half block */
    memcpy((byte*)newNode+newOffset, &isLeaf, 1); newOffset += 1;
    memcpy((byte*)newNode+newOffset, &newN, 4); newOffset += 4;
    memcpy((byte*)newNode+newOffset, &newF, 4); newOffset += 4;
    memcpy((byte*)newNode+newOffset, (byte*)currentPage + SecondHalfStart, SecondHalfLength);

    /* update N and F in current node */
    memcpy((byte*)currentPage + 1, &N, 4);
    memcpy((byte*)currentPage + 5, &F, 4);

    return 0;

}

RC ix_helpers::splitNodeIntoTwoLeaf(IXFileHandle &ixFileHandle, void *currentPage, const Attribute &attribute, void *newNode, unsigned& newPageNum) {
//only for leaf

    byte isLeaf=0; unsigned N=0, F=0, newN=0, newF=0, offset=0, newOffset=0, SecondHalfStart=0;
    int nextLeaf=0, newNextLeaf=0;

    memcpy(&isLeaf, (byte*)currentPage + offset, 1); offset += 1;
    if(isLeaf == 0){ //input node is not intermediate, return error
        std::cerr << "This is for leaf node" << std::endl;
        return 1;
    }
    memcpy(&N, (byte*)currentPage + offset, 4); offset += 4;
    memcpy(&F, (byte*)currentPage+ offset, 4); offset += 4;
    memcpy(&nextLeaf, (byte*)currentPage+ offset, 4); offset += 4;

    for(int i=0 ; i < N ; i++){
        // [ K_i(key,rid) ]

        if(attribute.type == TypeInt || attribute.type == TypeReal){
            offset += 4;
        }else if(attribute.type == TypeVarChar) {
            unsigned  currentKeyLength=0;
            memcpy(&currentKeyLength, (byte *)currentPage + offset, 4);
            offset += 4 + currentKeyLength;
        }

        offset += 8; // for rid

        if( offset > (PAGE_SIZE/2) ) {
            SecondHalfStart = offset;
            newN = N - (i + 1); N = i+1;
            break;
        }
    }

    unsigned SecondHalfLength = (PAGE_SIZE - F) - SecondHalfStart;
    newF = PAGE_SIZE - 13 - SecondHalfLength;
    F = F + SecondHalfLength;
    newNextLeaf = nextLeaf;

    /* Format newNode and move the second half block */
    memcpy((byte*)newNode+newOffset, &isLeaf, 1); newOffset += 1;
    memcpy((byte*)newNode+newOffset, &newN, 4); newOffset += 4;
    memcpy((byte*)newNode+newOffset, &newF, 4); newOffset += 4;
    memcpy((byte*)newNode+newOffset, &newNextLeaf, 4); newOffset += 4;
    memcpy((byte*)newNode+newOffset, (byte*)currentPage + SecondHalfStart, SecondHalfLength);

    // to get the pagenum of new node, we need ixFileHandle as an input, and append the page for new node here.
    RC rc = ixFileHandle.appendPage(newNode);
    if(rc!=0) return 1;
    newPageNum = ixFileHandle.getNumberOfPages()-1;
    nextLeaf = newPageNum;

    /* update N, F, and nextLeaf in current node */
    memcpy((byte*)currentPage + 1, &N, 4);
    memcpy((byte*)currentPage + 5, &F, 4);
    memcpy((byte*)currentPage + 9, &nextLeaf, 4);

    return 0;
}

/***
 *
 * @param btreenode
 * @param attribute
 * @param key
 * @param rid
 * @param retEntryOffset
 * @return
 *          IX_NO_ENTRIES if N == 0,
 *          IX_SCAN_ENTRY_FOUND if key is max until
 *          IX_SCAN_ENTRY_FOUND_EOP if key is max until but is last entry
 *          IX_SCAN_NOT_FOUND if key is larger than last entry
 *          exit(-77) if unexpected error
 */
RC ix_helpers::searchEntryInclusive(const byte *btreenode,
                                    const Attribute &attribute, const void *key, const RID &rid, int &retEntryOffset) {
    /* Initialize variables */
    int cursor = 0;
    byte isLeaf = 0; unsigned n = 0;

    memcpy(&isLeaf, btreenode + cursor, 1); cursor += 1;
    memcpy(&n, btreenode + cursor, 4); cursor += 4;
    cursor += 4;    // Skip F

    const unsigned N = n;

    if (N == 0) return IX_NO_ENTRIES;

    int extraJumps = 0;     // For "jumping" over pointer p_i when in intermediate nodes
    if (isLeaf == 0) { // Intermediate node
        extraJumps = 4;     // Jump over p_i
    }

    /* Set cursor to point to first entry */
    cursor += 4;

    /* Search for entry
     * Edge case for search */
    int previousEntryLen = 0;
    for (unsigned i = 0; i < N; ++i) {
        RID entryRID;
        int entryLen = 0;
        if (attribute.type == TypeInt || attribute.type == TypeReal) {
            entryLen = 4;
        }
        else if (attribute.type == TypeVarChar) {
            memcpy(&entryLen, btreenode + cursor, 4);
            entryLen += 4;      // For first int value
        }
        memcpy(&entryRID.pageNum, btreenode + cursor + entryLen, 4);
        memcpy(&entryRID.slotNum, btreenode + cursor + entryLen + 4, 4);
        entryLen += 8;      // For RID

        int compResult = ix_helpers::keyCompare(attribute, key, rid, btreenode + cursor, entryRID);

        if ((compResult == 0) || (compResult < 0)) {      // If equal or < (<=)
            if ((i+1) < N) {
                retEntryOffset = cursor;
                return IX_SCAN_ENTRY_FOUND;
            }
            else {
                retEntryOffset = cursor;
                return IX_SCAN_ENTRY_FOUND_EOP;
            }
        }
        else {
            if ((i+1) < N) {
                previousEntryLen = entryLen;
                cursor += (previousEntryLen     // Jump over entry
                           + extraJumps);      // Jump over p_i if it is intermediate node
            } else {        // Reached last entry
                retEntryOffset = cursor;
                return IX_SCAN_NOT_FOUND;
            }
        }
    }

    std::cerr << "Unexpected error in searchEntryInclusive()" << std::endl;
    exit(-77);
}

RC ix_helpers::searchExactEntry(const byte *btreenode, const Attribute &attribute, const void *key, const RID &rid,
                                int &retEntryOffset) {
    // Error handling
    if (btreenode == nullptr) return IX_ERROR;

    /* Initialize variables */
    int cursor = 0;
    byte isLeaf = 0; unsigned n = 0;

    memcpy(&isLeaf, btreenode + cursor, 1); cursor += 1;
    memcpy(&n, btreenode + cursor, 4); cursor += 4;
    cursor += 4;    // Skip F

    const unsigned N = n;

    if (N == 0) return IX_ERROR;

    int extraJumps = 0;     // For "jumping" over pointer p_i when in intermediate nodes
    if (isLeaf == 0) { // Intermediate node
        extraJumps = 4;     // Jump over p_i
    }

    /* Set cursor to point to first entry */
    cursor += 4;

    /* Search for entry
     * Edge case for search */
    int previousEntryLen = 0;
    for (unsigned i = 0; i < N; ++i) {
        RID entryRID;
        int entryLen = 0;
        if (attribute.type == TypeInt || attribute.type == TypeReal) {
            entryLen = 4;
        }
        else if (attribute.type == TypeVarChar) {
            memcpy(&entryLen, btreenode + cursor, 4);
            entryLen += 4;      // For first int value
        }
        memcpy(&entryRID.pageNum, btreenode + cursor + entryLen, 4);
        memcpy(&entryRID.slotNum, btreenode + cursor + entryLen + 4, 4);
        entryLen += 8;      // For RID

        int compResult = ix_helpers::keyCompare(attribute, key, rid, btreenode + cursor, entryRID);

        if (compResult == 0) {      // If equal or < (<=)
            retEntryOffset = cursor;
            return IX_OK;
        }
        else {
            if ((i+1) < N) {
                previousEntryLen = entryLen;
                cursor += (previousEntryLen     // Jump over entry
                           + extraJumps);      // Jump over p_i if it is intermediate node
            } else {        // Reached last entry
                retEntryOffset = cursor;
                return IX_SEARCH_NOT_FOUND;
            }
        }
    }

    return IX_ERROR;
}

int ix_helpers::keyCompare(const Attribute &attribute, const void *_lhs, const RID &lhs_rid, const void *_rhs,
                           const RID &rhs_rid) {
    int compareResult = 0;

    if (attribute.type == TypeInt) {
        int lhsInt = 0;
        memcpy(&lhsInt, _lhs, 4);

        int rhsInt = -1;
        memcpy(&rhsInt, _rhs, 4);

        if (lhsInt < rhsInt) compareResult = -1;
        else if (lhsInt == rhsInt) {
            if (lhs_rid < rhs_rid) compareResult = -1;
            else if (lhs_rid == rhs_rid) compareResult = 0;
            else compareResult = 1;
        }
        else compareResult = 1;
    } else if (attribute.type == TypeReal) {
        float lhsReal = 0;
        memcpy(&lhsReal, _lhs, 4);

        float rhsReal = -1;
        memcpy(&rhsReal, _rhs, 4);

        if (lhsReal < rhsReal) compareResult = -1;
        else if (lhsReal == rhsReal) {
            if (lhs_rid < rhs_rid) compareResult = -1;
            else if (lhs_rid == rhs_rid) compareResult = 0;
            else compareResult = 1;
        }
        else compareResult = 1;
    } else if (attribute.type == TypeVarChar) {
        int lhsLen = 0;
        memcpy(&lhsLen, _lhs, 4);
        char* lhsVarChar = new char[lhsLen + 1];
        memcpy(lhsVarChar, (char*)_lhs + 4, lhsLen);
        lhsVarChar[lhsLen] = '\0';

        int rhsLen = 0;
        memcpy(&rhsLen, _rhs, 4);
        char* rhsVarChar = new char[rhsLen + 1];
        memcpy(rhsVarChar, (char*)_rhs + 4, rhsLen);
        rhsVarChar[rhsLen] = '\0';

        compareResult = strcmp(lhsVarChar, rhsVarChar);
        if (compareResult == 0) {
            if (lhs_rid < rhs_rid) compareResult = -1;
            else if (lhs_rid == rhs_rid) compareResult = 0;
            else compareResult = 1;
        }
        delete[] lhsVarChar;
        delete[] rhsVarChar;
    }

    return compareResult;
}

RC ix_helpers::chooseSubtree(byte *btreenode, const Attribute &attribute, const void *key, const RID &keyRID,
                             int &retOffset) {
    /* Get offset */
    int offset = -1;
    RC rc = ix_helpers::searchEntryInclusive(btreenode, attribute, key, keyRID, offset);
    if (rc == IX_ERROR) {
        return IX_ERROR;
    } else if (rc == IX_NO_ENTRIES) {
        return IX_NO_ENTRIES;
    }

    /* Compare key values */
    int rhsRIDOffset = -1;
    if (attribute.type == TypeInt || attribute.type == TypeReal) {
        rhsRIDOffset = offset + 4;
    } else if (attribute.type == TypeVarChar) {
        int charlen = -1;
        memcpy(&charlen, btreenode + offset, 4);
        rhsRIDOffset = offset + 4 + charlen;
    }
    RID rhs;
    memcpy(&rhs.pageNum, btreenode + rhsRIDOffset, 4); rhsRIDOffset += 4;
    memcpy(&rhs.slotNum, btreenode + rhsRIDOffset, 4); rhsRIDOffset += 4;

    int compResult = ix_helpers::keyCompare(attribute, key, keyRID, btreenode + offset, rhs);

    if (compResult < 0) {
        retOffset = (offset-4);
        return IX_OK;
    } else {
        retOffset = (rhsRIDOffset);
        return IX_OK;
    }
}

RC ix_helpers::removeEntry(byte *btreenode, const Attribute &attribute, const void *key, const RID &keyRID) {
    /* Consume headers */
    int offset = 0;
    byte isLeaf = 0; unsigned N = 0; unsigned F = 0;
    memcpy(&isLeaf, btreenode, 1); offset += 1;
    memcpy(&N, btreenode + offset, 4); offset += 4;
    memcpy(&F, btreenode + offset, 4);

    /* Check if N is 0 */
    if (N == 0) {
        std::cerr << "removeEntry(): N is 0" << std::endl;
        return IX_SEARCH_NOT_FOUND;
    }

    /* Search for entry */
    int keyEntryOffset = -1;
    RC rc = searchExactEntry(btreenode, attribute, key, keyRID, keyEntryOffset);
    if (rc != IX_OK) {
        std::cerr << "removeEntry(): rc is not IX_OK, rc: " << rc << std::endl;
        return IX_SEARCH_NOT_FOUND;
    }

    /* Get length of key and rid */
    int keyLen = 0;
    if (attribute.type == TypeReal || attribute.type == TypeInt) {
        keyLen = 4 + 8;
    } else if (attribute.type == TypeVarChar) {
        int temp = 0;
        memcpy(&temp, key, 4);
        keyLen = 4 + temp + 8;
    }

    /* Check if leaf */
    if (isLeaf == 1) {
        int nodeEndOffset = PAGE_SIZE - F;
        offset = (keyEntryOffset + keyLen);

        int movingSize = nodeEndOffset - offset;
        memmove(btreenode + keyEntryOffset, btreenode + offset, movingSize);

        /* Update F and N*/
        F += keyLen;
        N -= 1;
        offset = 1;
        memcpy(btreenode + offset, &N, 4); offset += 4;
        memcpy(btreenode + offset, &F, 4);
    } else {
        int nodeEndOffset = PAGE_SIZE - F;
        offset = (keyEntryOffset + keyLen + 4);     // Add pointer

        int movingSize = nodeEndOffset - offset;
        memmove(btreenode + keyEntryOffset, btreenode + offset, movingSize);

        /* Update F and N*/
        F += (keyLen + 4);
        N -= 1;
        offset = 1;
        memcpy(btreenode + offset, &N, 4); offset += 4;
        memcpy(btreenode + offset, &F, 4);
    }

    return IX_OK;
}

////////////// IndexManager Definitions ///////////////

IndexManager &IndexManager::instance() {
    static IndexManager _index_manager = IndexManager();
    return _index_manager;
}

RC IndexManager::createFile(const std::string &fileName) {

    if(pfm_helpers::fileExists(fileName)) return 1;

    FILE* fptr = nullptr;
    fptr = fopen(fileName.c_str(), "wb");
    if(fptr == nullptr) return 1;

    unsigned zeroInitialValues = 0;
    unsigned init = 1;

    /* Write hidden page
     * has meta-data in this order:
     * 0-3: ixReadPageCounter
     * 4-7: ixWritePageCounter
     * 8-11: ixAppendPageCounter
     * 12-15: rootPageNum
     * 16-19: init */
    byte buffer[PAGE_SIZE] = { 0 }; unsigned offset = 0;

    memcpy(buffer + offset, &zeroInitialValues, 4); offset += 4;
    memcpy(buffer + offset, &zeroInitialValues, 4); offset += 4;
    memcpy(buffer + offset, &zeroInitialValues, 4); offset += 4;
    memcpy(buffer + offset, &zeroInitialValues, 4); offset += 4;
    memcpy(buffer + offset, &init, 4); offset += 4;

    /*
    pfm_helpers::unsignedIntToBytes(zeroInitialValues, buffer, 0); // for ixReadPageCounter
    pfm_helpers::unsignedIntToBytes(zeroInitialValues, buffer, 4); // for ixWritePageCounter
    pfm_helpers::unsignedIntToBytes(zeroInitialValues, buffer, 8); // for ixAppendPageCounter
    pfm_helpers::unsignedIntToBytes(zeroInitialValues, buffer, 12); // for rootPageNum
    pfm_helpers::unsignedIntToBytes(init, buffer, 16); // for init
    */

    auto writtenBytes = fwrite(buffer, sizeof(byte), PAGE_SIZE, fptr);
    if(writtenBytes != PAGE_SIZE) return 1;

    fclose(fptr);
    return 0;

}

RC IndexManager::destroyFile(const std::string &fileName) {

    if(!pfm_helpers::fileExists(fileName)) return 1;

    if(remove(fileName.c_str()) != 0){ return 1;}
    else{ return 0; }

}

RC IndexManager::openFile(const std::string &fileName, IXFileHandle &ixFileHandle) {

    /* Check if fileHandle is already associated with a file */
    if (ixFileHandle.filePointer != nullptr) {
        return 1;
    }

    /* Try to open file */
    FILE* fptr = fopen(fileName.c_str(), "rb+");
    if (fptr == nullptr) {
        return 1;
    }

    /* Load hidden page to memory
     * Hidden page has the format like:
     * [ ixReadPageCounter(4) | ixWritePageCounter(4) | ixAppendPageCounter(4) | rootPageNum(4) | init(4) ] */
    byte buffer[PAGE_SIZE] = { 0 };
    auto readBytes = fread(buffer, sizeof(byte), PAGE_SIZE, fptr);
    if (readBytes != PAGE_SIZE) {
        return 1;
    }

    /* Set the ixFileHandle variables */
    unsigned offset = 0;
    memcpy(&(ixFileHandle.ixReadPageCounter), buffer + offset, 4); offset += 4;
    memcpy(&(ixFileHandle.ixWritePageCounter), buffer + offset, 4); offset += 4;
    memcpy(&(ixFileHandle.ixAppendPageCounter), buffer + offset, 4); offset += 4;
    memcpy(&(ixFileHandle.rootPageNum), buffer + offset, 4); offset += 4;
    memcpy(&(ixFileHandle.init), buffer + offset, 4); offset += 4;
    ixFileHandle.thisFileName = fileName;
    ixFileHandle.filePointer = fptr;

    /*
    ixFileHandle.ixReadPageCounter = pfm_helpers::bytesToUnsignedInt(buffer, 0);
    ixFileHandle.ixWritePageCounter = pfm_helpers::bytesToUnsignedInt(buffer, 4);
    ixFileHandle.ixAppendPageCounter = pfm_helpers::bytesToUnsignedInt(buffer, 8);
    ixFileHandle.rootPageNum = pfm_helpers::bytesToUnsignedInt(buffer, 12);
    ixFileHandle.init = pfm_helpers::bytesToUnsignedInt(buffer, 16);
    ixFileHandle.filePointer = fptr;
     */

    return 0;

}

RC IndexManager::closeFile(IXFileHandle &ixFileHandle) {

    /* Check if fileHandle is associated with a file */
    if (ixFileHandle.filePointer == nullptr) {
        return 1;
    }

    /* Write counters to hidden page */
    byte buffer[PAGE_SIZE] = { 0 }; unsigned offset = 0;

    memcpy(buffer + offset, &(ixFileHandle.ixReadPageCounter), 4); offset += 4;
    memcpy(buffer + offset, &(ixFileHandle.ixWritePageCounter), 4); offset += 4;
    memcpy(buffer + offset, &(ixFileHandle.ixAppendPageCounter), 4); offset += 4;
    memcpy(buffer + offset, &(ixFileHandle.rootPageNum), 4); offset += 4;
    memcpy(buffer + offset, &(ixFileHandle.init), 4); offset += 4;

    /*
    pfm_helpers::unsignedIntToBytes(ixFileHandle.ixReadPageCounter, buffer, 0);
    pfm_helpers::unsignedIntToBytes(ixFileHandle.ixWritePageCounter, buffer, 4);
    pfm_helpers::unsignedIntToBytes(ixFileHandle.ixAppendPageCounter, buffer, 8);
    pfm_helpers::unsignedIntToBytes(ixFileHandle.rootPageNum, buffer, 12);
    pfm_helpers::unsignedIntToBytes(ixFileHandle.init, buffer, 16);
     */

    /* Before closing file, need to save counter values
     * For that, move filePointer to the beginning of the file, */
    int seekResult = fseek(ixFileHandle.filePointer, 0, SEEK_SET);
    if (seekResult != 0){
        return 1;
    }

    /* and write them in the first hidden page */
    auto writtenBytes = fwrite(buffer, sizeof(byte), PAGE_SIZE, ixFileHandle.filePointer);
    if (writtenBytes != PAGE_SIZE) {
        return 1;
    }

    /* Close filePointer */
    fclose(ixFileHandle.filePointer);
    ixFileHandle.filePointer = nullptr;

    return 0;

}


RC IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
    if(ixFileHandle.filePointer == nullptr){
        std::cerr << "fileHandle is not associated with a file" << std::endl;
        return 1;
    }

    if(ixFileHandle.init == 1){ // This is the first insert after creating file.

        RC rc = ix_helpers::insertInit(ixFileHandle, attribute, key, rid);
        if(rc != 0){
            std::cerr << "insertInit error" << std::endl;
            return 1;
        }

    }else if(ixFileHandle.init == 0){ // for any insertion after root set (init == 0)

        void* newChildEntry = malloc(PAGE_SIZE);
        RC rc = ix_helpers::insertTree(ixFileHandle, ixFileHandle.rootPageNum, attribute, key, rid, newChildEntry);
        if(rc != 0){
            std::cerr << "insertTree error" << std::endl;
            return 1;
        }
        free(newChildEntry);
    }

    /* Re-initialize active iterators */
    auto it = IX_globals::activeIterators.begin();
    while (it != IX_globals::activeIterators.end()) {
        auto scanPtr = it->second;
        scanPtr->reInitialize();
        it++;
    }

    return 0;
}

RC IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
    /* Read root page */
    PageNum rootPageNum = ixFileHandle.rootPageNum;
    byte rootPageBuf[PAGE_SIZE];
    if (ixFileHandle.readPage(rootPageNum, rootPageBuf) != IX_OK) return IX_ERROR;

    /* Consume headers */
    byte isLeaf = 255;
    unsigned N = 0;
    unsigned F = 0;
    memcpy(&isLeaf, rootPageBuf, 1);
    memcpy(&N, rootPageBuf + 1, 4);
    memcpy(&F, rootPageBuf + 5, 4);

    /* Check if leaf */
    if (isLeaf == 1) {
        /* If leaf, do removeEntry */
        RC rc = ix_helpers::removeEntry(rootPageBuf, attribute, key, rid);
        if (rc == IX_SEARCH_NOT_FOUND) {
            return IX_SEARCH_NOT_FOUND;
        }

        /* Flush node to disk and return*/
        ixFileHandle.writePage(rootPageNum, rootPageBuf);

        /* Re-initialize active iterators */
        auto it = IX_globals::activeIterators.begin();
        while (it != IX_globals::activeIterators.end()) {
            auto scanPtr = it->second;
            scanPtr->reInitialize();
            it++;
        }

        return IX_OK;
    } else if (isLeaf == 0) {
        /* If not leaf, find appropriate leaf */
        PageNum leafNodeNum = BtreeSearch(ixFileHandle, rootPageNum, attribute, key, rid);

        /* Read the leaf node */
        byte leafNodeBuf[PAGE_SIZE];
        ixFileHandle.readPage(leafNodeNum, leafNodeBuf);

        /* Delete entry */
        RC rc = ix_helpers::removeEntry(leafNodeBuf, attribute, key, rid);
        if (rc == IX_SEARCH_NOT_FOUND) {
            return IX_SEARCH_NOT_FOUND;
        }

        /* Flush to disk and return */
        ixFileHandle.writePage(leafNodeNum, leafNodeBuf);

        /* Re-initialize active iterators */
        auto it = IX_globals::activeIterators.begin();
        while (it != IX_globals::activeIterators.end()) {
            auto scanPtr = it->second;
            scanPtr->reInitialize();
            it++;
        }

        return IX_OK;
    } else return IX_ERROR;
}

RC IndexManager::scan(IXFileHandle &ixFileHandle,
                      const Attribute &attribute,
                      const void *loKey,
                      const void *hiKey,
                      bool lowKeyInclusive,
                      bool highKeyInclusive,
                      IX_ScanIterator &ix_ScanIterator) {

    /* Check if ixFileHandle is associated with a file */
    if (ixFileHandle.filePointer == nullptr) {
        return 1;
    }

    /* Get the root of the tree */
    PageNum rootPageNum = ixFileHandle.rootPageNum;

    /* Set lowRID according to inclusive flags */
    RID lowRID;
    if (lowKeyInclusive) {
        lowRID.pageNum = 0;
        lowRID.slotNum = 0;
    } else {
        lowRID.pageNum = UINT_MAX;
        lowRID.slotNum = UINT_MAX;
    }

    /* Copy lowkey and highkey values */
    void *lowKey = nullptr; void *highKey = nullptr;
    if (loKey == NULL) {
        if (attribute.type == TypeInt) {
            lowKey = malloc(4);
            int neginfinity = INT_MIN;
            memcpy(lowKey, &neginfinity, 4);
        } else if (attribute.type == TypeReal) {
            lowKey = malloc(4);
            float neginf = FLT_MIN;
            memcpy(lowKey, &neginf, 4);
        } else if (attribute.type == TypeVarChar) {
            lowKey = malloc(5);
            int len = 1;
            memcpy(lowKey, &len, 4);
            char negInf = 0;
            memcpy((byte*)lowKey + 4, &negInf, len);
        }
    } else {
        if(attribute.type == TypeInt || attribute.type == TypeReal){
            lowKey = malloc(4);
            memcpy(lowKey, loKey, 4);
        } else if(attribute.type == TypeVarChar){
            int len = -1;
            memcpy(&len, loKey, 4);

            lowKey = malloc(len + 4);
            memcpy(lowKey, &len, 4);
            memcpy((byte*)lowKey + 4, (byte*)loKey + 4, len);
        }
    }
    if (hiKey == NULL) {
        if (attribute.type == TypeInt) {
            highKey = malloc(4);
            int infinity = INT_MAX;
            memcpy(highKey, &infinity, 4);
        } else if (attribute.type == TypeReal) {
            highKey = malloc(4);
            float inf = FLT_MAX;
            memcpy(highKey, &inf, 4);
        } else if (attribute.type == TypeVarChar) {
            highKey = malloc(5);
            int len = 1;
            memcpy(highKey, &len, 4);
            char inf = CHAR_MAX;
            memcpy((byte*)highKey + 4, &inf, len);
        }
    } else {
        if(attribute.type == TypeInt || attribute.type == TypeReal){
            highKey = malloc(4);
            memcpy(highKey, hiKey, 4);
        } else if(attribute.type == TypeVarChar){
            int len = -1;
            memcpy(&len, hiKey, 4);

            highKey = malloc(len + 4);
            memcpy(highKey, &len, 4);
            memcpy((byte*)highKey + 4, (byte*)hiKey + 4, len);
        }
    }

    /* Find the starting node for the scan, maintain this starting node number */
    PageNum startNodeNum = BtreeSearch(ixFileHandle, rootPageNum, attribute, lowKey, lowRID);
    byte startNodeBuff[PAGE_SIZE];
    ixFileHandle.readPage(startNodeNum, startNodeBuff);     // FIXME: error-handling

    /* Error handling for non-leaf nodes */
    byte isLeaf = 0;
    memcpy(&isLeaf, startNodeBuff, 1);
    if (isLeaf != 1) {
        std::cerr << "Error: scan must be on leafs!!" << std::endl;
        return IX_ERROR;
    }

    /* Find the starting entry offset */
    int nextEntryOffset = -1;
    RC rc = ix_helpers::searchEntryInclusive(startNodeBuff, attribute, lowKey, lowRID, nextEntryOffset);

    /* Enumerate cases where we should advance the nextEntryOffset */
    if (rc == IX_SCAN_NOT_FOUND || rc == IX_NO_ENTRIES) {
        /* Get next page number */
        int nextLeaf = -1;
        int nextLeafOffset = (1 + 4 + 4);       // Skip headers
        memcpy(&nextLeaf, startNodeBuff + nextLeafOffset, 4); nextLeafOffset += 4;

        /* Initialize N */
        unsigned N = 0;

        /* Keep advancing page until N is not 0 */
        while (N == 0) {
            if (nextLeaf == -1) {
                ix_ScanIterator.setIsEOF(true);     // End of file reached
                break;
            } else {
                startNodeNum = nextLeaf;
                rc = ixFileHandle.readPage(startNodeNum, startNodeBuff);
                if (rc != 0) {
                    std::cerr << "Error on readPage()" << std::endl;
                    return rc;
                }

                nextEntryOffset = nextLeafOffset;               // Should always be 13
                memcpy(&N, startNodeBuff + 1, 4);
                memcpy(&nextLeaf, startNodeBuff + 9, 4);
            }
        }
    }

    /* Initialize scan iterator */
    ix_ScanIterator.initScanIterator(ixFileHandle, startNodeNum, attribute, highKey, highKeyInclusive);
    ix_ScanIterator.setCursor(nextEntryOffset);

    free(lowKey);
    return IX_OK;
}

PageNum IndexManager::BtreeSearch(IXFileHandle &ixFileHandle, const PageNum CUR_NODE_NUM,
                                    const Attribute &attribute, const void *keyValue, const RID &rid)
{
    /* Read page, we want to control the life cycle of the page buffer */
    byte btreeNode[PAGE_SIZE];
    ixFileHandle.readPage(CUR_NODE_NUM, btreeNode);

    /* Base case: if leaf return */
    byte isLeaf = 0;
    memcpy(&isLeaf, btreeNode, 1);
    if (isLeaf == 1) {
        return CUR_NODE_NUM;
    }

    /* Decide which subtree to go to */
    int subtreeOffset = -1;
    RC rc = ix_helpers::chooseSubtree(btreeNode, attribute, keyValue, rid, subtreeOffset);
    if (rc == IX_NO_ENTRIES) {
        std::cerr << "Somethings wrong in BTreeSearch.. Intermediate nodes should not be empty!!" << std::endl;
        exit(-1);
    }

    PageNum subtreeNnum = 0;
    memcpy(&subtreeNnum, btreeNode + subtreeOffset, 4);
    return BtreeSearch(ixFileHandle, subtreeNnum, attribute, keyValue, rid);
}



void IndexManager::printBtree(IXFileHandle &ixFileHandle, const Attribute &attribute) const {
    printPrettyBtree(ixFileHandle, ixFileHandle.rootPageNum, attribute, 0);
    std::cout << std::endl;
}

void IndexManager::printPrettyBtree(IXFileHandle &ixFileHandle, unsigned nodePtr, const Attribute &attribute, int level) const {
    /* Read page */
    byte pageBuffer[PAGE_SIZE];
    ixFileHandle.readPage(nodePtr, pageBuffer);

    /* Consume headers */
    int cursor = 0;
    byte isLeaf = 0; unsigned N = 0;
    memcpy(&isLeaf, pageBuffer, 1); cursor += 1;
    memcpy(&N, pageBuffer + cursor, 4); cursor += 4;

    /* Indent */
    for (int i = 0; i < level; ++i) {std::cout << "    "; std::cout.flush();}
    std::cout << "{"; std::cout.flush();

    /* Print keys */
    printKeys(pageBuffer, attribute, nodePtr);

    /* Check leaf node */
    if (isLeaf == 1) {
        std::cout << "}"; std::cout.flush();
        return;
    } else {
        std::cout << "," << std::endl;
        for (int i = 0; i < level; ++i) {std::cout << "    "; std::cout.flush();}      // Indent
        std::cout << " \"children\": [" << std::endl;
    }

    /* Start printing children */
    cursor += 4;        // Skip F
    const int CHILDREN = (int)N+1;
    for (int j = 0; j < CHILDREN; ++j) {
        unsigned childPtr = 0;
        memcpy(&childPtr, pageBuffer+cursor, 4); cursor += 4;

        printPrettyBtree(ixFileHandle, childPtr, attribute, level+1);

        /* Check if child is not last */
        if ((j+1) < CHILDREN) {
            std::cout << ","; std::cout.flush();
            if ((attribute.type == TypeInt) || (attribute.type == TypeReal)) {
                cursor += 4;        // skip value
                cursor += 8;        // skip RID
            } else if (attribute.type == TypeVarChar) {
                int keyLen = 0;
                memcpy(&keyLen, pageBuffer + cursor, 4);
                cursor += 4;        // skip len
                cursor += keyLen;   // skip string
                cursor += 8;        // skip RID
            }
        }
        std::cout << std::endl;
    }

    for (int i = 0; i < level; ++i) {std::cout << "    "; std::cout.flush();}
    std::cout << "]}"; std::cout.flush();
}

void IndexManager::printKeys(const byte* pageBuffer, const Attribute &attribute, unsigned pagenum) const {
    /* Consume header */
    unsigned isLeaf = 0, N = 0;      // Variables
    int cursor = 0;     // offset

    memcpy(&isLeaf, pageBuffer, 1); cursor += 1;
    memcpy(&N, pageBuffer + cursor, 4); cursor += 4;
    cursor += 4;        // Skip F

    /* Check isLeaf */
    int extraJump = 0;
    if (isLeaf == 0) {
        extraJump = 4;
    }

    /* Skip to next entry */
    cursor += 4;

    /* Print object array */
    std::cout << "\"keys\": ["; std::cout.flush();

    float curFloatKey = 0;
    char curVarCharKey[PAGE_SIZE];
    int curIntKey = 0;
    for (unsigned i = 0; i < N; ++i) {
        /* Print key */
        if(attribute.type == TypeInt) {
            int key = 0;
            memcpy(&key, pageBuffer + cursor, 4); cursor += 4;
            if (i == 0) {
                curIntKey = key;
                std::cout << "\"" << curIntKey << ":" << "["; std::cout.flush();
            }

            if (curIntKey != key) {
                curIntKey = key;
                std::cout << "\"" << curIntKey << ":" << "["; std::cout.flush();

                RID rid;
                memcpy(&rid.pageNum, pageBuffer + cursor, 4); cursor += 4;
                memcpy(&rid.slotNum, pageBuffer + cursor, 4); cursor += 4;
                std::cout << "(" << rid.pageNum << "," << rid.slotNum << ")"; std::cout.flush();
                cursor += extraJump;
            } else {            //  if (curIntKey == key)
                RID rid;
                memcpy(&rid.pageNum, pageBuffer + cursor, 4); cursor += 4;
                memcpy(&rid.slotNum, pageBuffer + cursor, 4); cursor += 4;
                std::cout << "(" << rid.pageNum << "," << rid.slotNum << ")"; std::cout.flush();
                cursor += extraJump;
            }

            /* Peek next key */
            int peek = 0;
            if ((i+1) < N) {
                memcpy(&peek, pageBuffer + cursor, 4);
                if (peek == curIntKey) {std::cout << ","; std::cout.flush();}
                else {std::cout << "]\","; std::cout.flush();}
            } else {std::cout << "]\""; std::cout.flush();}
        } else if(attribute.type == TypeReal) {
            float key = 0;
            memcpy(&key, pageBuffer + cursor, 4); cursor += 4;
            if (i == 0) {
                curFloatKey = key;
                std::cout << "\"" << curFloatKey << ":" << "["; std::cout.flush();
            }

            if (curFloatKey != key) {
                curFloatKey = key;
                std::cout << "\"" << curFloatKey << ":" << "["; std::cout.flush();

                RID rid;
                memcpy(&rid.pageNum, pageBuffer + cursor, 4); cursor += 4;
                memcpy(&rid.slotNum, pageBuffer + cursor, 4); cursor += 4;
                std::cout << "(" << rid.pageNum << "," << rid.slotNum << ")"; std::cout.flush();
                cursor += extraJump;
            } else {            //  if (curFloatKey == key)
                RID rid;
                memcpy(&rid.pageNum, pageBuffer + cursor, 4); cursor += 4;
                memcpy(&rid.slotNum, pageBuffer + cursor, 4); cursor += 4;
                std::cout << "(" << rid.pageNum << "," << rid.slotNum << ")"; std::cout.flush();
                cursor += extraJump;
            }

            /* Peek next key */
            float peek = 0;
            if ((i+1) < N) {
                memcpy(&peek, pageBuffer + cursor, 4);
                if (peek == curFloatKey) {std::cout << ","; std::cout.flush();}
                else {std::cout << "]\","; std::cout.flush();}
            } else {std::cout << "]\""; std::cout.flush();}
        } else if(attribute.type == TypeVarChar) {
            int keyLen = 0;
            memcpy(&keyLen, pageBuffer + cursor, 4); cursor += 4;

            char* key = new char[keyLen + 1];
            memcpy(key, pageBuffer + cursor, keyLen); cursor += keyLen;
            key[keyLen] = '\0';

            if (i == 0) {
                strcpy(curVarCharKey, key);
                std::cout << "\"" << curVarCharKey << ":" << "["; std::cout.flush();
            }

            if (strcmp(curVarCharKey, key) != 0) {
                strcpy(curVarCharKey, key);
                std::cout << "\"" << curVarCharKey << ":" << "["; std::cout.flush();

                RID rid;
                memcpy(&rid.pageNum, pageBuffer + cursor, 4); cursor += 4;
                memcpy(&rid.slotNum, pageBuffer + cursor, 4); cursor += 4;
                std::cout << "(" << rid.pageNum << "," << rid.slotNum << ")"; std::cout.flush();
                cursor += extraJump;
            } else {            //  if (curFloatKey == key)
                RID rid;
                memcpy(&rid.pageNum, pageBuffer + cursor, 4); cursor += 4;
                memcpy(&rid.slotNum, pageBuffer + cursor, 4); cursor += 4;
                std::cout << "(" << rid.pageNum << "," << rid.slotNum << ")"; std::cout.flush();
                cursor += extraJump;
            }

            /* Peek next key */
            int peekLen = 0; char* peekChar = nullptr;
            if ((i+1) < N) {
                memcpy(&peekLen, pageBuffer + cursor, 4);
                peekChar = new char[peekLen + 1];
                memcpy(peekChar, pageBuffer + cursor + 4, peekLen);
                peekChar[peekLen] = '\0';
                if (strcmp(peekChar, curVarCharKey) == 0) {std::cout << ","; std::cout.flush();}
                else {std::cout << "]\","; std::cout.flush();}

            } else {std::cout << "]\""; std::cout.flush();}

            delete[] peekChar;
            delete[] key;
        }
    }

    std::cout << "]";
}

void IndexManager::printBtreeDebug(IXFileHandle &ixFileHandle, const Attribute &attribute) const {
    printPrettyBtreeDebug(ixFileHandle, ixFileHandle.rootPageNum, attribute, 0);
    std::cout << std::endl;
}

void IndexManager::printPrettyBtreeDebug(IXFileHandle &ixFileHandle, unsigned nodePtr, const Attribute &attribute, int level) const {
    /* Read page */
    byte pageBuffer[PAGE_SIZE];
    ixFileHandle.readPage(nodePtr, pageBuffer);

    /* Consume headers */
    int cursor = 0;
    byte isLeaf = 0; unsigned N = 0;
    memcpy(&isLeaf, pageBuffer, 1); cursor += 1;
    memcpy(&N, pageBuffer + cursor, 4); cursor += 4;

    /* Indent */
    for (int i = 0; i < level; ++i) {std::cout << "    "; std::cout.flush();}
    std::cout << "{"; std::cout.flush();

    /* Print header info */
    byte isLeafDebug = 0;       memcpy(&isLeafDebug, pageBuffer, 1);
    unsigned NDebug = 0;        memcpy(&NDebug, pageBuffer + 1, 4);
    unsigned FDebug = 0;        memcpy(&FDebug, pageBuffer + 5, 4);
    int Variable = 0;      memcpy(&Variable, pageBuffer + 9, 4);
    std::cout << (int)isLeafDebug << "|" << NDebug << "|" << FDebug << "|" << Variable << " -- PageNum: " << nodePtr << std::endl;
    for (int i = 0; i < level; ++i) {std::cout << "    "; std::cout.flush();}

    /* Print keys */
    printKeys(pageBuffer, attribute, nodePtr);

    /* Check leaf node */
    if (isLeaf == 1) {
        std::cout << "}"; std::cout.flush();
        return;
    } else {
        std::cout << "," << std::endl;
        for (int i = 0; i < level; ++i) {std::cout << "    "; std::cout.flush();}      // Indent
        std::cout << " \"children\": [" << std::endl;
    }

    /* Start printing children */
    cursor += 4;        // Skip F
    const int CHILDREN = (int)N+1;
    for (int j = 0; j < CHILDREN; ++j) {
        unsigned childPtr = 0;
        memcpy(&childPtr, pageBuffer+cursor, 4); cursor += 4;

        printPrettyBtreeDebug(ixFileHandle, childPtr, attribute, level+1);

        /* Check if child is not last */
        if ((j+1) < CHILDREN) {
            std::cout << ","; std::cout.flush();
            if ((attribute.type == TypeInt) || (attribute.type == TypeReal)) {
                cursor += 4;        // skip value
                cursor += 8;        // skip RID
            } else if (attribute.type == TypeVarChar) {
                int keyLen = 0;
                memcpy(&keyLen, pageBuffer + cursor, 4);
                cursor += 4;        // skip len
                cursor += keyLen;   // skip string
                cursor += 8;        // skip RID
            }
        }
        std::cout << std::endl;
    }

    for (int i = 0; i < level; ++i) {std::cout << "    "; std::cout.flush();}
    std::cout << "]}"; std::cout.flush();
}

void IndexManager::printBtreeDebug(std::string indexFilename, const Attribute &attribute) {
    IXFileHandle ixFileHandle;
    openFile(indexFilename, ixFileHandle);

    printPrettyBtreeDebug(ixFileHandle, ixFileHandle.rootPageNum, attribute, 0);
    std::cout << std::endl;
}

////////////// IX_ScanIterator Definitions ///////////////




IX_ScanIterator::IX_ScanIterator()  {
    nextEntryOffset = -1;
    isEOF = false;
    prevReturnedEntry = nullptr;
    //ixFileHandle;
}

IX_ScanIterator::~IX_ScanIterator() {
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
    if (isEOF) {
        return IX_EOF;
    }

    /* Read current page into memory */
    byte pageBuffer[PAGE_SIZE];
    RC rc = ixFileHandle.readPage(currentNodeNum, pageBuffer);
    if (rc != IX_OK) {
        std::cerr << "Error in readPage() from getNextEntry()" << std::endl;
        return IX_ERROR;
    }

    /* Consume headers */
    byte isLeaf = 0;        memcpy(&isLeaf, pageBuffer, 1);
    if (isLeaf != 1) {std::cerr << "Can only scan leaves!! " << std::endl; exit(-45);}
    unsigned N = 0;         memcpy(&N, pageBuffer + 1, 4);
    unsigned F = 0;         memcpy(&F, pageBuffer + 5, 4);
    int nextLeaf = 0;       memcpy(&nextLeaf, pageBuffer + 9, 4);

    RID currentRID;
    void *currentKey = nullptr;
    int currentKeyLen = 0;

    /* Read current entry and RID */
    if (attribute.type == TypeVarChar) {
        int len = 0;
        memcpy(&len, pageBuffer + nextEntryOffset, 4);
        nextEntryOffset += 4;

        currentKeyLen = len + 4;
        currentKey = malloc(currentKeyLen);

        memcpy(currentKey, &len, 4);
        memcpy((byte*)currentKey + 4, pageBuffer + nextEntryOffset, len);    // copy string
        nextEntryOffset += len;

        memcpy(&currentRID.pageNum, pageBuffer + nextEntryOffset, 4); nextEntryOffset += 4;
        memcpy(&currentRID.slotNum, pageBuffer + nextEntryOffset, 4); nextEntryOffset += 4;
    } else {
        currentKeyLen = 4;
        currentKey = malloc(currentKeyLen);
        memcpy(currentKey, pageBuffer + nextEntryOffset, 4); nextEntryOffset += 4;
        memcpy(&currentRID.pageNum, pageBuffer + nextEntryOffset, 4); nextEntryOffset += 4;
        memcpy(&currentRID.slotNum, pageBuffer + nextEntryOffset, 4); nextEntryOffset += 4;
    }

    /* Check if highKey is reached */
    if (attribute.type == TypeInt) {
        int intcurkey = 0;
        memcpy(&intcurkey, currentKey, 4);

        int hiIntKey = 1;
        memcpy(&hiIntKey, highKey, 4);

        if (highKeyInclusive) {
            if (intcurkey > hiIntKey) {
                isEOF = true;
            }
        } else {
            if (intcurkey >= hiIntKey) {
                isEOF = true;
            }
        }
    } else if (attribute.type == TypeReal) {
        float realcurkey = 0;
        memcpy(&realcurkey, currentKey, 4);

        float hiRealKey = 1;
        memcpy(&hiRealKey, highKey, 4);

        if (highKeyInclusive) {
            if (realcurkey > hiRealKey) {
                isEOF = true;
            }
        } else {
            if (realcurkey >= hiRealKey) {
                isEOF = true;
            }
        }
    } else if (attribute.type == TypeVarChar) {
        int keyLen = 0;
        memcpy(&keyLen, currentKey, 4);
        char* varCharKey = new char[keyLen + 1];
        memcpy(varCharKey, (byte*)currentKey + 4, keyLen);
        varCharKey[keyLen] = '\0';

        int hiKeyLen = 0;
        memcpy(&hiKeyLen, highKey, 4);
        char* hiVarChar = new char[hiKeyLen + 1];
        memcpy(hiVarChar, (byte*)highKey + 4, hiKeyLen);
        hiVarChar[hiKeyLen] = '\0';

        int compResult = strcmp(varCharKey, hiVarChar);

        if (highKeyInclusive) {
            if (compResult > 0) {
                isEOF = true;
            }
        } else {
            if (compResult >= 0) {
                isEOF = true;
            }
        }
        delete[] varCharKey;
        delete[] hiVarChar;
    }

    if (isEOF) {
        free(currentKey);
        return IX_EOF;
    }

    /* Return next entry stuff */
    memcpy(key, currentKey, currentKeyLen);
    rid = currentRID;

    /* Save to prevReturnedEntry */
    if (prevReturnedEntry != nullptr) {free(prevReturnedEntry); prevReturnedEntry = nullptr;}
    prevReturnedEntry = malloc(currentKeyLen);
    memcpy(prevReturnedEntry, currentKey, currentKeyLen);
    prevReturnedRID = currentRID;


    /* Check bounds */
    if ((PAGE_SIZE - (unsigned)nextEntryOffset) == F) {
        /* Get next page number */
        int nextLeafOffset = (1 + 4 + 4);       // Skip headers
        memcpy(&nextLeaf, pageBuffer + nextLeafOffset, 4); nextLeafOffset += 4;

        /* Initialize N */
        N = 0;

        /* Keep advancing page until N is not 0 */
        while (N == 0) {
            if (nextLeaf == -1) {
                isEOF = true;
                break;
            } else {
                currentNodeNum = nextLeaf;
                rc = ixFileHandle.readPage(currentNodeNum, pageBuffer);
                if (rc != 0) {
                    std::cerr << "Error on readPage()" << std::endl;
                    return rc;
                }

                nextEntryOffset = nextLeafOffset;               // Should always be 13
                memcpy(&N, pageBuffer + 1, 4);
                memcpy(&nextLeaf, pageBuffer + 9, 4);
            }
        }
    }

    free(currentKey);
    return IX_OK;
}

RC IX_ScanIterator::close() {
    if (highKey != NULL) {
        free(highKey);
        highKey = NULL;
    }
    if (prevReturnedEntry != nullptr) {
        free(prevReturnedEntry);
        prevReturnedEntry = nullptr;
    }
    IX_globals::activeIterators.erase(ID);

    ID = -1;
    nextEntryOffset = -1;
    currentNodeNum = 0;
    isEOF = false;

    return 0;
}

RC IX_ScanIterator::initScanIterator(IXFileHandle ixFH, PageNum nodeNum,
                                        const Attribute &attr,
                                        void *hi, bool hiInclusive) {
    if (ID < 0) {
        ID = IX_globals::iteratorIDs++;
        IX_globals::activeIterators.insert(std::pair<int, IX_ScanIterator*>(ID, this));
    }

    ixFileHandle = ixFH;
    currentNodeNum = nodeNum;

    attribute = attr;
    highKey = hi;
    highKeyInclusive = hiInclusive;

    return 0;
}

void IX_ScanIterator::setIsEOF(bool val) {
    isEOF = val;
}

RC IX_ScanIterator::setCursor(int nextEntry) {
    nextEntryOffset = nextEntry;
    return 0;
}

void IX_ScanIterator::reInitialize() {
    if (!isEOF) {
        /* Get lowRID */
        RID lowRID = prevReturnedRID;
        lowRID.slotNum += 1;
        const void *lowKey = prevReturnedEntry;

        /* Get the root of the tree */
        PageNum rootPageNum = ixFileHandle.rootPageNum;

        /* Find the starting node for the scan, maintain this starting node number */
        currentNodeNum = IndexManager::instance().BtreeSearch(ixFileHandle, rootPageNum, attribute, lowKey, lowRID);
        byte startNodeBuff[PAGE_SIZE];
        ixFileHandle.readPage(currentNodeNum, startNodeBuff);     // FIXME: error-handling

        /* Find the starting entry offset */
        RC rc = ix_helpers::searchEntryInclusive(startNodeBuff, attribute, lowKey, lowRID, nextEntryOffset);

        /* Enumerate cases where we should advance the nextEntryOffset */
        if (rc == IX_SCAN_NOT_FOUND || rc == IX_NO_ENTRIES) {
            /* Get next page number */
            int nextLeaf = -1;
            int nextLeafOffset = (1 + 4 + 4);       // Skip headers
            memcpy(&nextLeaf, startNodeBuff + nextLeafOffset, 4); nextLeafOffset += 4;

            /* Initialize N */
            unsigned N = 0;

            /* Keep advancing page until N is not 0 */
            while (N == 0) {
                if (nextLeaf == -1) {
                    isEOF = true;
                    break;
                } else {
                    currentNodeNum = nextLeaf;
                    rc = ixFileHandle.readPage(currentNodeNum, startNodeBuff);
                    if (rc != 0) {
                        std::cerr << "Error on readPage()" << std::endl;
                        exit(-1);
                    }

                    nextEntryOffset = nextLeafOffset;               // Should always be 13
                    memcpy(&N, startNodeBuff + 1, 4);
                    memcpy(&nextLeaf, startNodeBuff + 9, 4);
                }
            }
        }
    }
}

////////////// IXFileHandle Definitions ///////////////

IXFileHandle::IXFileHandle() {

    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;

    filePointer = nullptr;
    rootPageNum = 0;
    init = 1;
}

IXFileHandle::~IXFileHandle() {}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {

    readPageCount = this->ixReadPageCounter;
    writePageCount = this->ixWritePageCounter;
    appendPageCount = this->ixAppendPageCounter;

    return 0;
}

RC IXFileHandle::readPage(PageNum pageNum, void *data){

    /* Page existence check */
    if(pageNum >= this->ixAppendPageCounter){
        return 1;
    }

    /* Reposition the filePointer
     * As the first page is for hidden page, need to go (PageNum + 1)-th page */
    int seekResult = fseek(this->filePointer, (pageNum+1)*PAGE_SIZE, SEEK_SET);
    if (seekResult != 0){
        return 1;
    }

    /* Copy one page from the file (pointed by filePointer) to the data */
    auto readBytes = fread(data, sizeof(byte), PAGE_SIZE, this->filePointer);
    if (readBytes != PAGE_SIZE) {
        return 1;
    }

    /* Increase the readPageCounter value */
    this->ixReadPageCounter = this->ixReadPageCounter + 1;

    /* Update the counter value in file's hidden page */
    int updateResult = ix_helpers::UpdateVariables(*this);
    if(updateResult != 0){
        return 2;
    }

    return 0;
}

RC IXFileHandle::writePage(PageNum pageNum, const void *data){

    /* Page existence check */
    if(pageNum >= this->ixAppendPageCounter){
        return 1;
    }

    /* Reposition the filePointer
     * As the first page is for hidden page, need to write in (PageNum + 1)-th page */
    int seekResult = fseek(this->filePointer, (pageNum+1)*PAGE_SIZE, SEEK_SET);
    if (seekResult != 0){
        return 1;
    }

    /* Write the data into one page in the file (pointed by filePointer)
     * As the first page is for hidden page, need to write in (PageNum + 1)-th page */
    auto writtenBytes = fwrite(data, sizeof(byte), PAGE_SIZE, this->filePointer);
    if (writtenBytes != PAGE_SIZE) {
        return 1;
    }

    /* Increase the writePageCounter value */
    this->ixWritePageCounter = this->ixWritePageCounter + 1;

    /* Update the counter value in file's hidden page */
    int updateResult = ix_helpers::UpdateVariables(*this);
    if(updateResult != 0){
        return 2;
    }

    return 0;

}

RC IXFileHandle::appendPage(const void *data){

    /* Reposition filePointer to the end of the file */
    int seekResult = fseek(this->filePointer, 0, SEEK_END);
    if (seekResult != 0){
        return 1;
    }

    /* Append the data into the last page of the file */
    auto writtenBytes = fwrite(data, sizeof(byte), PAGE_SIZE, this->filePointer);
    if (writtenBytes != PAGE_SIZE) {
        return 1;
    }

    /* Increase the appendPageCounter value */
    this->ixAppendPageCounter = this->ixAppendPageCounter + 1;

    /* Update the counter value in file's hidden page */
    int updateResult = ix_helpers::UpdateVariables(*this);
    if(updateResult != 0){
        return 2;
    }

    return 0;

}


unsigned IXFileHandle::getNumberOfPages() {

    long start, end;

    /* Seek the beginning of the file and save the position into start */
    int seekResult = fseek(this->filePointer, 0, SEEK_SET);
    if (seekResult != 0){
        return 1;
    }
    start = ftell(this->filePointer);

    /* Seek the end of the file and save the position into end */
    seekResult = fseek(this->filePointer, 0, SEEK_END);
    if (seekResult != 0){
        return 1;
    }
    end = ftell(this->filePointer);

    return (end - start)/PAGE_SIZE - 1;

}