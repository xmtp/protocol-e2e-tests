#!/bin/bash

while true; do 
	cargo xdbg -d -b dev generate --entity group --amount 1 --concurrency 1
	cargo xdbg -d -b dev generate --entity message --amount 1 --concurrency 1
	sleep 60
done
