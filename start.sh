#!/bin/bash
mvn exec:java -Dexec.mainClass="dbpedia.Main" -Dexec.args="/home/milan/tmp/dbpedia-abstracts/" > /home/milan/tmp/dbpedia-abstracts/log.txt 2>&1 &