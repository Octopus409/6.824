package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	LeaderId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.LeaderId = int(nrand()) % len(servers) // 随机挑选servers某个服务器发送
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{
		Key: key,
	}
	reply := GetReply{}
	for {
		ok := ck.servers[ck.LeaderId].Call("KVServer.Get", &args, &reply)
		if !ok { // 发送失败
			ck.LeaderId = int(nrand()) % len(ck.servers)
			continue
		}
		if reply.Err == ErrWrongLeader { // 不是leader
			ck.LeaderId = reply.LeaderId
			continue
		}
		if reply.Err == ErrNoKey {
			return ""
		}
		return reply.Value
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
	}
	reply := PutAppendReply{}
	for {
		ok := ck.servers[ck.LeaderId].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			ck.LeaderId = int(nrand()) % len(ck.servers)
			continue
		}
		if reply.Err == ErrWrongLeader {
			ck.LeaderId = reply.LeaderId
			continue
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
