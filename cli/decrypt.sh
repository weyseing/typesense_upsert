#!/bin/bash

REPO_PATH="$HOME/temp/typesense_upsert" # EDIT HERE

openssl enc -d -aes-256-cbc -in $REPO_PATH/typesense_app.enc -out $REPO_PATH/typesense_app.zip
unzip $REPO_PATH/typesense_app.zip
rm -rf $REPO_PATH/typesense_app.zip
echo "Decryption complete"