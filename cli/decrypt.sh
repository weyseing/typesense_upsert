#!/bin/bash

REPO_PATH="$HOME/temp/typesense_upsert" # EDIT HERE

openssl enc -d -aes-256-cbc -in $REPO_PATH/typesense.enc -out $REPO_PATH/typesense.zip
unzip $REPO_PATH/typesense.zip
rm -rf $REPO_PATH/typesense.zip
echo "Decryption complete"