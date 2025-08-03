#!/bin/bash

REPO_PATH="$HOME/temp/typesense_upsert" # EDIT HERE

cd "$REPO_PATH" && zip -r typesense.zip typesense
openssl enc -aes-256-cbc -salt -in $REPO_PATH/typesense.zip -out $REPO_PATH/typesense.enc
rm -rf $REPO_PATH/typesense.zip
echo "Encryption complete"