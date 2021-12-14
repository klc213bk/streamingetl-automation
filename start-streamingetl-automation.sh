#!/bin/bash

java -jar -Dspring.profiles.active=dev target/streamingetl-automation-1.0.jar --spring.config.location=file:config/ 
