#!/bin/bash
/bin/rm -rf api
(cd ../target/scala-2.11; tar -cf - ./api) | tar -xf -
