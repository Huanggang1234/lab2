package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "sync/atomic"
import "6.5840/labrpc"
//import "6.5840/labgob"
import "math/rand"
import "time"
import "fmt"
//import "bytes"

// import "bytes"
// import "../labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}


const (
    follower=iota
    candidate
	leader
)

//
// A Go object implementing a single Raft peer.
//

type LogEntry struct{
    Term   int
	Index  int
	Command interface {}
}

var noneLog=LogEntry{0,0,nil}

type Entry struct{
    Log LogEntry    //实际的日志
    ArrayIndex int  //数组索引
    None  bool      //是否是空指令
    Count int       //写入计数，超过一半则可以提交
    Rindex int      
}

var entry=Entry{noneLog,0,true,1,0}

type Raft struct{
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
        msgChan   chan ApplyMsg
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
    
	cond       *sync.Cond

        voteFor     int  //投票给了谁
        mainPeers   int  //大多数节点数量
	leader      int  //当前的领导者
	curTerm     int  //自己所处的任期
	status      int  //当前的身份
	timeBeat    int  //心跳间隔时间
	timeBase    int  //基础超时时间
	timeout     int  //心跳超时时间
	timeAccu    int
	maxTerm     int 
	voting      bool //是否处于活跃状态
	appok      bool

        /***************2B***********************/
	addTerm     int
        logs    []Entry
	logLen  int //日志数组的长度，同时也是下一条日志存放的位置
	index   int //下一条日志索引
	commited int //提交索引
	leaIndex int //当期领导者第一条日志索引
	nextIndex  []int
	lastApplied int
	mmu    sync.Mutex
	ccond  *sync.Cond
}

func(rf *Raft) emptyLog() *Entry{
         log:=new(Entry)
	 log.Log=noneLog
	 log.ArrayIndex=rf.logLen
	 log.None=true
	 log.Count=1
	 log.Log.Term=rf.curTerm
	 log.Rindex=rf.index
	 return log
} 

func(rf *Raft) ResetTime(){
    rf.timeout=rf.timeBase*(rand.Intn(100)+100)*1.0/100.0
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool){

	var term int
	var isleader bool
	// Your code here (2A).

        rf.mu.Lock()
	term=rf.curTerm
        isleader=rf.me==rf.leader
        rf.mu.Unlock()

        if isleader {
	    fmt.Printf("Get state raft %d is leader of %d logLen:%d term:%d commited:%d apply:%d\n",rf.me,rf.curTerm,rf.logLen,rf.logs[rf.logLen-1].Log.Term,rf.commited,rf.lastApplied)
 	}else{
	    fmt.Printf("Get state raft %d is %d of %d the leader=%d logLen:%d term:%d commited:%d apply:%d\n",rf.me,rf.status,rf.curTerm,rf.leader,rf.logLen,rf.logs[rf.logLen-1].Log.Term,rf.commited,rf.lastApplied)
 	}

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
/*
         w:=new(bytes.Buffer)
	 e:=labgob.NewEncoder(w)
	 e.Encode(rf.voteFor)
	 e.Encode(rf.curTerm)
	 e.Encode(rf.commited)
//	 e.Encode(rf.lastApplied)
//	 e.Encode(rf.index)
//	 e.Encode(rf.logs)
	 data:=w.Bytes()
	 fmt.Printf("persist() data len:%d\n",len(data))
//	 rf.persister.SaveRaftState(data)
*/
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
	   fmt.Printf("readPersist(): %d\n",len(data))
	   return
	}
/*
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

        r:=bytes.NewBuffer(data)
	d:=labgob.NewDecoder(r)
        var voteFor int
	var curTerm int
	var commited int
//	var lastApplied int
//	var index int
//	var logs   []Entry

        err:=d.Decode(&voteFor)
	err=d.Decode(&curTerm)
	err=d.Decode(&commited)
//	err=d.Decode(&lastApplied)
//	err=d.Decode(&index)
//	err=d.Decode(&logs)
        
	if err!=nil {
           fmt.Printf("readPersist error\n")
	   return
	}else{
           rf.voteFor=voteFor
	   rf.curTerm=curTerm
	   rf.commited=commited
//	   rf.lastApplied=lastApplied
//	   rf.index=index
//	   rf.logs=logs
	}
	fmt.Printf("%d %d %d %d %d\n",rf.voteFor,rf.curTerm,rf.commited,rf.lastApplied,rf.index)
*/
}




// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
    Id  int  //候选者id
    Term  int //候选者的任期
    LastLogTerm int
    LastLogIndex int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
    Vote  bool //是否投票给他 //如果Vote为true则忽略其余参数
    CurTerm  int //当前任期
    Old   bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply){
	// Your code here (2A, 2B).

     reply.CurTerm=rf.curTerm
     reply.Vote=false
     reply.Old=false

     rf.mu.Lock()
     defer rf.mu.Unlock()  

     if args.Term < rf.curTerm  {
	      return
     }

     ok:=true
     index:=rf.logLen-1
     if args.LastLogTerm < rf.logs[index].Log.Term || ((args.LastLogTerm==rf.logs[index].Log.Term&&
	 args.LastLogIndex < rf.logs[index].ArrayIndex)){
          fmt.Printf("%d refuse %d for log[%d]:",rf.me,args.Id,index)
          fmt.Println(args,"  ",rf.logs[index])
          ok=false
	  reply.Old=true
	  if rf.status==candidate {
             rf.addTerm++
	  }
     }

     switch(rf.status){
	 
	     case follower:
	          
		  if (rf.voteFor==-1 || rf.voteFor==args.Id ||args.Term >rf.curTerm)&&ok{
		         reply.CurTerm=args.Term
			 reply.Vote=true
			 rf.voteFor=args.Id
			 rf.curTerm=args.Term
                         rf.leader=-1
			 rf.timeAccu=0
		  } else{
                         fmt.Printf("switch %d refuse %d for voteFor==%d or voteFor!=%d or args.Term:%d > rf.curTerm:%d\n",
			 rf.me,args.Id,rf.voteFor,args.Id,args.Term,rf.curTerm)
	          }
           //       fmt.Printf("%d flush time for vote from %d\n",rf.me,args.Id)

	     case candidate:

		  if rf.voting {
		      if args.Term > rf.maxTerm {
			 rf.maxTerm = args.Term
		      }
		      return
		  }
		
		  if (rf.voteFor==-1 || rf.voteFor==args.Id || args.Term >rf.curTerm)&&ok {
			  rf.curTerm=args.Term
			  rf.voteFor=args.Id
			  rf.status=follower
			  reply.CurTerm=args.Term
			  reply.Vote=true
			  rf.cond.Broadcast()
		  }

	     case leader:

		  if args.Term > rf.curTerm {
		     fmt.Printf("leader %d ->follower for vote for %d\n",rf.me,args.Id)
		     rf.status=follower
		     rf.curTerm=args.Term
		     rf.leader=-1
		     reply.CurTerm=args.Term
		     if ok {
		       rf.voteFor=args.Id
		       reply.Vote=true
		     }else{
		       rf.status=candidate
		       rf.curTerm++
		       rf.voteFor=-1
		       rf.cond.Broadcast()
		     }
		  }
     }

      if reply.Vote {

	  fmt.Printf("%d voteFor %d args term:%d index:%d  my term:%d index:%d\n",rf.me,args.Id,args.LastLogTerm,args.LastLogIndex,rf.logs[index].Log.Term,rf.logs[index].ArrayIndex)
      }

     return
}

const (
   heart=iota
   task
)

type AppendEntriesArgs struct{
     NextLogIndex  int
     LastLogIndex  int
     LastLogTerm   int
     CurTerm       int //领导者当前的任期
     LeaderId      int
     MsgType       int
     Commited      int
     Log         Entry
}

type AppendEntriesReply struct{
     Success bool
     Repeat  bool //日志是否重复添加
     CurTerm  int
}



//添加日志条目的rpc，同时用作心跳包,现在不检查日志条目
func(rf *Raft) AppendEntries(args *AppendEntriesArgs,reply *AppendEntriesReply){

     rf.mu.Lock()
     defer rf.mu.Unlock()

     reply.CurTerm=rf.curTerm
     reply.Success=false
     reply.Repeat=false
     if args.CurTerm < rf.curTerm {
	return
     }

     reply.CurTerm=args.CurTerm
     if args.CurTerm > rf.curTerm {
	rf.voteFor=-1
     }
     rf.curTerm=args.CurTerm
     preStatus:=rf.status
     rf.status=follower
     rf.leader=args.LeaderId
     if preStatus==candidate {
        rf.cond.Broadcast()
     }
     
     if preStatus==leader {
        rf.cond.Broadcast()
     }

     reply.Success=true
     rf.timeAccu=0
//     fmt.Printf("%d flush time for app from %d\n",rf.me,args.LeaderId) 
     /********************2B**********************/

     if args.NextLogIndex>rf.logLen {
 
//	fmt.Printf("%d refuse %d app for args.NextLogIndex:%d > rf.logLen:%d\n",rf.me,args.LeaderId,args.NextLogIndex,rf.logLen)
	reply.Success=false
        return
     }

     next:=args.NextLogIndex
     index:=args.LastLogIndex
     term:=args.LastLogTerm
     
     if rf.logs[next-1].ArrayIndex!=index||rf.logs[next-1].Log.Term!=term{
//	 fmt.Printf("%d refuse %d (%d) app for arrayIndex:%d != Lastindex:%d or myTerm:%d != term:%d\n",rf.me,args.LeaderId,preStatus,rf.logs[next-1].ArrayIndex,index,
//	   rf.logs[next-1].Log.Term,term)
	 reply.Success=false

         if rf.logs[next-1].Log.Term!=term{
//	    fmt.Printf("%d index %d term:%d != %d\n",rf.me,next-1,rf.logs[next-1].Log.Term,term)
	    rf.logs=rf.logs[:next-1]
	    rf.logLen=len(rf.logs)
	    rf.index=rf.logs[rf.logLen-1].Rindex
	 }

	 return
     }

     if rf.logLen > next && rf.logs[next].ArrayIndex==args.Log.ArrayIndex && rf.logs[next].Log.Term == args.Log.Log.Term {
         reply.Repeat=true
     }

     if (!reply.Repeat)&&args.MsgType==task {
	 rf.logs=rf.logs[:next]
	 rf.logs=append(rf.logs,args.Log)
	 rf.logLen=len(rf.logs) 
	 rf.index=rf.logs[rf.logLen-1].Rindex
	 rf.timeAccu=0
     }

       
     var commited int
     if args.MsgType==task {
	if args.Commited >= next {
	   commited=next
	}else{
	   commited=args.Commited
	}
     }else{
	if args.Commited >=index{
	   commited=index
	}else{
	   commited=args.Commited
	}
     }


     if rf.commited < commited {
	rf.mmu.Lock()
	rf.commited=commited
	rf.mmu.Unlock()
	rf.ccond.Broadcast()
     }


        fmt.Printf("%d agree %d (%d)append the index:%d term:%d \n",rf.me,args.LeaderId,preStatus,args.Log.ArrayIndex,args.Log.Log.Term)
//        fmt.Print(reply.Repeat)
//	fmt.Printf(" commited now:%d\n args commite %d pre %d:%d next %d:%d\n",rf.commited,args.Commited,args.LastLogIndex,args.LastLogTerm,args.Log.ArrayIndex,args.Log.Log.Term)
}

//在调用make时，作为后台进程调用
func(rf *Raft) Work(){

    var pre int
    var log *Entry
    for !rf.killed() {
	    
	rf.mu.Lock()
	switch rf.status {
	
	    case follower:
		    rf.timeAccu=0
		    rf.ResetTime()
		    rf.playFollower()
		     
   	    case candidate:

	             rf.leader=-1
	             rf.playCandidate()

            case leader:
		 rf.leader=rf.me
     
		 /******************2b*****************/
		 pre=rf.logLen
		 log=rf.emptyLog()
		 rf.logs=append(rf.logs,*log)
		 rf.logLen=len(rf.logs)
		 rf.leaIndex=pre
		 for i,_:=range rf.peers {
		     if i==rf.me {
			continue
		     }
		     rf.nextIndex[i]=pre
		     go rf.leaderExecu(i)
		 }
	              /*************************************/
	         rf.playLeader()
        }
   }

}


func(rf *Raft) playFollower(){
 
     rf.mu.Unlock()  
     rf.timeAccu=0
     for rf.timeAccu < rf.timeout {
	     time.Sleep(time.Millisecond*time.Duration(10))
	     rf.timeAccu+=10
     }
     rf.mu.Lock()
   //  fmt.Printf("raft %d follower -> candidate for outHeartBeat\n",rf.me)
     rf.status=candidate
     rf.mu.Unlock()
}


func(rf *Raft) countThread(){
     rf.ResetTime()
     time.Sleep(time.Duration(rf.timeout)*time.Millisecond)
     rf.voting=false
     rf.cond.Broadcast()
}

func(rf *Raft) playCandidate(){
      rf.addTerm=1 
      for rf.status==candidate {
	  rf.curTerm=rf.curTerm+rf.addTerm
	  term:=rf.curTerm
	  lastIndex:=rf.logs[rf.logLen-1].ArrayIndex
	  lastTerm:=rf.logs[rf.logLen-1].Log.Term
	  rf.maxTerm=rf.curTerm
	  rf.voteFor=rf.me
	  rf.mu.Unlock()

	  res:=len(rf.peers)-1
	  count:=1
	  old:=0
	  response:=1
	  fmt.Printf("%d make a vote of %d\n",rf.me,term)

	  rf.voting=true
          go rf.countThread()
	  for i,_:=range rf.peers {
		 if i == rf.me {
		    continue
		 }
		 args:=new(RequestVoteArgs)
		 reply:=new(RequestVoteReply)
		 args.Id=rf.me
		 args.Term=term
		 args.LastLogTerm=lastTerm
		 args.LastLogIndex=lastIndex
		 go rf.sendRequestVote(i,args,reply,&res,&count,&old,&response)
	  } 
	  rf.mu.Lock()
	  for rf.voting && res!=0 && !rf.killed(){
		 rf.cond.Wait()
	  }
          
	  if rf.killed(){
             rf.mu.Unlock()
	     return
	  }

	  rf.voting=false
	  fmt.Printf("%d'vote get %d tikets of term:%d old:%d response:%d\n",rf.me, count,term,old,response)
  
	  if count > rf.mainPeers {		 	   
		   fmt.Printf("%d become the leader of term:%d\n",rf.me,rf.maxTerm+2)
		   rf.curTerm=rf.maxTerm+3
		   rf.status=leader
		   rf.leader=rf.me
		   rf.mu.Unlock()
		   return
	  }
	  if rf.status!=candidate {
	         rf.mu.Unlock()
		 return
	  }
	  if rf.maxTerm > rf.curTerm {
	     rf.curTerm = rf.maxTerm
	     rf.voteFor=-1
	  }

          if old >=rf.mainPeers {
             for rf.status==candidate {
		 rf.cond.Wait()
	     }
	     rf.mu.Unlock()
	     return
	  }
	  rf.mu.Unlock()
	  rf.ResetTime()
 	  time.Sleep(time.Millisecond*time.Duration(rf.timeout))
	  rf.mu.Lock()
      }
      rf.mu.Unlock()
}

func(rf *Raft) countThread2(out*bool){
     for rf.status==leader {
         time.Sleep(time.Duration(rf.timeBeat)*time.Millisecond)
	 (*out)=true
	 rf.cond.Broadcast()
     }
}


func(rf *Raft) doLog(index int){
 //   fmt.Printf("dolog:index:%d reallen=%d  logLen=%d\n",index,len(rf.logs),rf.logLen)
    if !rf.logs[index].None {
       msg:=new(ApplyMsg)
       msg.CommandValid=true
       msg.Command=rf.logs[index].Log.Command
       msg.CommandIndex=rf.logs[index].Log.Index
       fmt.Printf("%d real apply arayindex:%d index:%d comamd:",rf.me,rf.logs[index].ArrayIndex,msg.CommandIndex)
       fmt.Println(msg.Command)
       rf.msgChan <- (*msg)
    }
}


func(rf *Raft) leaderExecu(wh int){
     
     rf.mu.Lock()

     for rf.status==leader&&!rf.killed(){
        index:=rf.nextIndex[wh]
	for index>=rf.logLen && !rf.killed(){//跟随者日志长度和领导者相同则等待新日志产生
            rf.cond.Wait()
	}
        if rf.status!=leader || rf.killed() {
	   rf.mu.Unlock()
	   return
	}
	curTerm:=rf.curTerm
	commited:=rf.commited
        rf.mu.Unlock()

        args:=new(AppendEntriesArgs)
        reply:=new(AppendEntriesReply)
 //       fmt.Printf("wh:%d nextIndex:%d  -1=%d\n",wh,index,index-1) 
        args.NextLogIndex=index
        args.LastLogIndex=rf.logs[index-1].ArrayIndex
	args.LastLogTerm=rf.logs[index-1].Log.Term
        args.CurTerm=curTerm
        args.LeaderId=rf.me
	args.MsgType=task
	args.Log=rf.logs[index]
        args.Commited=commited
        ok:=rf.peers[wh].Call("Raft.AppendEntries",args,reply)
        
        rf.mu.Lock()

        if rf.status!=leader {
           rf.mu.Unlock()
	   return
	}

        if ok {	
            if reply.Success {
		 rf.nextIndex[wh]=index+1
                 if !reply.Repeat{
		     rf.logs[index].Count++
		     if (rf.logs[index].Count-rf.mainPeers==1)&&index>=rf.leaIndex {
		       rf.mmu.Lock()
		       rf.commited=index
		       rf.mmu.Unlock()
		       rf.ccond.Broadcast()
		       fmt.Printf("%d commited index: %d count:%d\n",rf.me,index,rf.logs[index].Count)
		     }
		 }
	    }else if reply.CurTerm > rf.curTerm {
		rf.curTerm=reply.CurTerm
		rf.status=follower
		rf.leader=-1
		rf.voteFor=-1
		rf.mu.Unlock()
		return
	    }else{
  //              fmt.Printf("%d kown %d refuse him\n",rf.me,wh)
                rf.nextIndex[wh]=index-1			     			
	    }
	}else{		
	       rf.mu.Unlock()
	       time.Sleep(time.Millisecond*time.Duration(rf.timeBeat))		   
	       rf.mu.Lock()
	} 
    }

    rf.mu.Unlock()
}



func(rf *Raft) playLeader(){
          
          var nextIndex=make([]int,len(rf.nextIndex))
          first:=true
	  out:=false
	  for rf.status==leader &&!rf.killed(){ 
		  term:=rf.curTerm
		  commited:=rf.commited
		  copy(nextIndex,rf.nextIndex)
		  rf.mu.Unlock()
		  out=false
		  if first {
		     go rf.countThread2(&out)
		     first=false
	          }
		  for i,_:=range rf.peers {
		  
			if i==rf.me{
			   continue
			}
                        index:=nextIndex[i]
			args:=new(AppendEntriesArgs)
			reply:=new(AppendEntriesReply)
			args.NextLogIndex=index
			args.LastLogIndex=rf.logs[index-1].ArrayIndex
			args.LastLogTerm=rf.logs[index-1].Log.Term
			args.CurTerm=term
			args.LeaderId=rf.me
			args.MsgType=heart
			args.Commited=commited
			go rf.sendAppendEntries(i,args,reply)
		  }

		  rf.mu.Lock()
		  for !out&&!rf.killed(){
			rf.cond.Wait()
		  }

                  if rf.killed() {
                     rf.mu.Unlock()
		     return
		  }

		  if rf.status!=leader {//可能会后台转follower,通过其他领导者的心跳包
			 rf.mu.Unlock()
			 return
		  }
      }
      rf.mu.Unlock()
}


//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply,res *int,count *int,old *int, response *int){
    ok := rf.peers[server].Call("Raft.RequestVote", args, reply)	
    rf.mu.Lock()
    (*res)--
    if ok {
       
       (*response)++

       if reply.Old {
          (*old)++
       }
       if reply.Vote {
	  (*count)++
       }else if reply.CurTerm > rf.maxTerm {
	  rf.maxTerm=reply.CurTerm
       }
    }
    ok=(*res)==0
    rf.mu.Unlock()
    if ok {
      rf.cond.Broadcast()
    }
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply){

    ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)	
    if ok {	    
      if reply.CurTerm > rf.curTerm {
	   rf.curTerm=reply.CurTerm
	   rf.status=follower
	   rf.leader=-1
	   rf.voteFor=-1
      }
    }	
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
   //     fmt.Printf("raft %d call Start()\n",rf.me)
        rf.mu.Lock()
        term=rf.curTerm
	isLeader=rf.status==leader
	if isLeader {
           index=rf.index
	   rf.index++
           log:=rf.emptyLog()
	   log.Log.Index=index
	   log.Log.Command=command
	   log.Log.Term=term
	   log.ArrayIndex=rf.logLen
	   log.None=false
	   rf.logs=append(rf.logs,*log)
	   rf.logLen=len(rf.logs)
	   rf.cond.Broadcast()
	   fmt.Printf("leader %d of term:%d append arrayIndex:%d index:%d term:%d command:",rf.me,rf.curTerm,log.ArrayIndex,index,term)
	   fmt.Println(command)
	}
	rf.mu.Unlock()
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	fmt.Printf("raft %d was killed\n",rf.me)
	rf.cond.Broadcast()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//

func(rf *Raft) applyThread(){
     
     var app int
     var commited int
     var ll  int
     rf.mmu.Lock()
     for !rf.killed() {

         for rf.lastApplied >= rf.commited || rf.logLen <= (rf.lastApplied+1) {
             rf.ccond.Wait()
	 }
         app=rf.lastApplied
	 commited=rf.commited
	 ll=rf.logLen
         rf.mmu.Unlock()
         for app < commited && app+1 < ll {
             rf.doLog(app+1)
//	     fmt.Printf("%d apply the index:%d term:%d status:%d\n",rf.me,rf.logs[app+1].ArrayIndex,rf.logs[app+1].Log.Term,rf.status)
	     app++
	 }
	 rf.mmu.Lock()
	 rf.lastApplied=app
     }
     rf.mmu.Unlock()
}


func Make(peers []*labrpc.ClientEnd, me int,
     persister *Persister, applyCh chan ApplyMsg) *Raft {
     rf := &Raft{}
     rf.peers = peers
     rf.persister = persister
     rf.me = me

    fmt.Printf("create raft %d\n",me)

     // Your initialization code here (2A, 2B, 2C).
    rf.cond=sync.NewCond(&rf.mu)
    rf.voteFor=-1
     rf.leader=-1
     rf.curTerm=0
     rf.maxTerm=0
     rf.status=follower
     rf.mainPeers=len(peers)/2
    rf.timeBeat=55
    rf.timeBase=110

    /******************2B******************/
    rf.lastApplied=0
    rf.index=1
    rf.logs=append(rf.logs,*rf.emptyLog())
    rf.logLen=len(rf.logs)
    rf.msgChan=applyCh
    rf.commited=0 //默认提交了第一条空索引
    rf.nextIndex=make([]int,len(peers))
    rf.ccond=sync.NewCond(&rf.mmu)
     // initialize from state persisted before a crash
    rf.readPersist(persister.ReadRaftState()) 
    rf.logLen=len(rf.logs)

    go rf.applyThread()
    go rf.Work()
    return rf
}








