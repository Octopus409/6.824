package raft

import "log"

// Debugging
const debug = true

func initLog() {
	log.SetFlags(log.Lmicroseconds)
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if debug {
		log.Printf(format, a...)
	}
	return
}
