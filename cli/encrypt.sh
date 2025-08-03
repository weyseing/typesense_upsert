#!/bin/bash

REPO_PATH="$HOME/temp/typesense_upsert" # EDIT HERE

cd "$REPO_PATH" && zip -r typesense_app.zip typesense_app
openssl enc -aes-256-cbc -salt -in $REPO_PATH/typesense_app.zip -out $REPO_PATH/typesense_app.enc
rm -rf $REPO_PATH/typesense_app.zip
echo "Encryption complete"