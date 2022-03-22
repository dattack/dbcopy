#!/usr/bin/env bash
openssl aes-256-cbc -K $encrypted_668f14e812bc_key -iv $encrypted_668f14e812bc_iv -in .travis/codesigning.asc.enc -out .travis/codesigning.asc -d
gpg --fast-import .travis/codesigning.asc

