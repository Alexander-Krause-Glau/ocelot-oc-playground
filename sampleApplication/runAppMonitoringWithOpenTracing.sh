#!/bin/sh
java -Dinspectit.config.file-based.path="./" -javaagent:inspectit-ocelot-agent-0.6.jar -jar sampleApplication.jar

