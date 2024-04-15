#!/bin/bash

API_KEY='$2a$10$LiwZDQpo9LsUhSG3b48ck.3jUGeVaJ2CmFxAlMp03qa9EJ2kVits6' 
COLLECTION_ID='659a4d141f5677401f189ff8'

curl -XGET \
    -H "X-Master-key: $API_KEY" \
    "https://api.jsonbin.io/v3/c/$COLLECTION_ID/bins"