package raft

import "log"

// Debugging
const Debug = true

func initLog(){
	log.SetFlags(log.Lmicroseconds)
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
