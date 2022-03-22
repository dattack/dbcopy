#!/usr/bin/env bash
mvn deploy -P sign,build-extras --settings .travis/settings.xml

