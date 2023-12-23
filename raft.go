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
import "6.5840/labgob"
import "math/rand"
import "time"
import "fmt"
import "bytes"
//import "runtime"



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
    Applied  bool      //是否是空指令
    Count int       //写入计数，超过一半则可以提交
    Rindex int   
    Sindex int      //本任期的第一条日志的索引
}

var entry=Entry{noneLog,0,false,1,0,0}

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

        /***************2B***********************/
        logs    []Entry
	logLen  int //日志数组的长度，同时也是下一条日志存放的位置
	index   int //下一条日志索引
	commited int //提交索引
	leaIndex int //当期领导者第一条日志索引
	nextIndex  []int
	lastApplied int
	mmu    sync.Mutex
	ccond  *sync.Cond
	/****************2C********************/
        matchIndex []int
}

func(rf *Raft) emptyLog() *Entry{
         log:=new(Entry)
	 log.Log=noneLog
	 log.ArrayIndex=rf.logLen
	 log.Applied=false
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

         w:=new(bytes.Buffer)
	 e:=labgob.NewEncoder(w)
	 e.Encode(rf.voteFor)
	 e.Encode(rf.curTerm)
	 e.Encode(rf.logs)
	 data:=w.Bytes()
//	 fmt.Printf("%d persist() data len:%d\n",rf.me,len(data))
	 rf.persister.Save(data,nil)

}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
	   fmt.Printf("readPersist(): %d\n",len(data))
	   return
	}

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
	var logs   []Entry

        err:=d.Decode(&voteFor)
	err=d.Decode(&curTerm)
	err=d.Decode(&logs)
        
	if err!=nil {
           fmt.Printf("readPersist error\n")
	   return
	}else{
           rf.voteFor=voteFor
	   rf.curTerm=curTerm
	   rf.logs=logs
	   rf.logLen=len(logs)
	   rf.index=rf.logs[rf.logLen-1].Rindex
	}
	fmt.Printf("%d %d %d %d\n",rf.voteFor,rf.curTerm,rf.lastApplied,rf.index)

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
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply){
	// Your code here (2A, 2B).

     reply.CurTerm=rf.curTerm
     reply.Vote=false

     rf.mu.Lock()
     defer rf.mu.Unlock()  

     if args.Term < rf.curTerm  {
         return
     }
    
     ok:=true
     index:=rf.logLen-1
     preStatus:=rf.status
     if args.LastLogTerm < rf.logs[index].Log.Term || ((args.LastLogTerm==rf.logs[index].Log.Term&&
	 args.LastLogIndex < rf.logs[index].ArrayIndex)){
          fmt.Printf("%d refuse %d for log[%d]:",rf.me,args.Id,index)
          fmt.Println(args,"  ",rf.logs[index])
          ok=false
     }

     if rf.voteFor==-1 || rf.voteFor==args.Id || args.Term >rf.curTerm {
	 reply.CurTerm=args.Term
	 rf.voteFor=-1
	 rf.curTerm=args.Term
	 rf.status=follower
	 rf.persist()
	 if preStatus==candidate {
            rf.cond.Broadcast()
	 }
	 rf.leader=-1
         if ok {
	    reply.Vote=true
	    rf.voteFor=args.Id
	    rf.timeAccu=0
	    rf.persist()
         }
     }

     if reply.Vote {
	 fmt.Printf("%d voteFor %d args term:%d index:%d  my term:%d index:%d\n",rf.me,args.Id,args.LastLogTerm,args.LastLogIndex,rf.logs[index].Log.Term,rf.logs[index].ArrayIndex)
     }
     return
}


type AppendEntriesArgs struct{
     NextLogIndex  int
     LastLogIndex  int
     LastLogTerm   int
     CurTerm       int //领导者当前的任期
     LeaderId      int
     Commited      int
     Log           []Entry
}

type AppendEntriesReply struct{
     Success bool
     CurTerm  int
     Xterm    int
     Xindex   int
     Xlen     int
}



//添加日志条目的rpc，同时用作心跳包,现在不检查日志条目
func(rf *Raft) AppendEntries(args *AppendEntriesArgs,reply *AppendEntriesReply){

     rf.mu.Lock()
     defer rf.mu.Unlock()

     reply.CurTerm=rf.curTerm
     reply.Success=false
     reply.Xlen=rf.logLen
     if args.CurTerm < rf.curTerm {
	fmt.Printf("%d refuse %d app args:%d %d my:%d\n",rf.me,args.LeaderId,args.CurTerm,args.NextLogIndex,rf.curTerm)
        reply.Xterm=-1
	reply.Xindex=-1
	return
     }

     reply.CurTerm=args.CurTerm
     if args.CurTerm > rf.curTerm {
	rf.curTerm=args.CurTerm
	rf.voteFor=-1
	rf.persist()
     }

     preStatus:=rf.status
     rf.status=follower
     rf.leader=args.LeaderId
     if preStatus==candidate {
        rf.cond.Broadcast()
     }
     
     if preStatus==leader {
        rf.cond.Broadcast()
     }

     task:=args.Log!=nil
     reply.Success=true
     rf.timeAccu=0

     if args.NextLogIndex>rf.logLen {
 
	 fmt.Printf("%d refuse %d app for args.NextLogIndex:%d > rf.logLen:%d Sindex:%d term:%d\n",rf.me,args.LeaderId,args.NextLogIndex,rf.logLen,rf.logs[rf.logLen-1].Sindex,rf.logs[rf.logLen-1].Log.Term)
	reply.Success=false
	reply.Xterm=rf.logs[rf.logLen-1].Log.Term
	reply.Xindex=rf.logs[rf.logLen-1].Sindex
        return
     }

     next:=args.NextLogIndex
     index:=args.LastLogIndex
     term:=args.LastLogTerm
     
     if rf.logs[next-1].ArrayIndex!=index||rf.logs[next-1].Log.Term!=term{
	 fmt.Printf("%d refuse %d (%d) app for arrayIndex:%d != Lastindex:%d or myTerm:%d != term:%d Sindex:%d len:%d\n",rf.me,args.LeaderId,preStatus,rf.logs[next-1].ArrayIndex,index,
	   rf.logs[next-1].Log.Term,term,rf.logs[next-1].Sindex,rf.logLen)
	 reply.Success=false
         reply.Xterm=rf.logs[next-1].Log.Term
         reply.Xindex=rf.logs[next-1].Sindex
	 return
     }

     if task {
	     var i int=0
	     var length=len(args.Log)
	     for i=0;i<length;i++ {
		 fmt.Printf("%d next:%d i=%d next+i:%d rf.logLen:%d args %d:%d\n",rf.me,next,i,next+i,rf.logLen,args.Log[i].ArrayIndex,args.Log[i].Log.Term)
		 if next+i <rf.logLen {
		     fmt.Println("my: ",rf.logs[next+i])
		 }
                 if (next+i <rf.logLen)&&rf.logs[next+i].ArrayIndex==args.Log[i].ArrayIndex && rf.logs[next+i].Log.Term==args.Log[i].Log.Term {
		     continue
		 }
		 rf.logs=rf.logs[:next+i]
		 rf.logLen=len(rf.logs)
		 rf.index=rf.logs[rf.logLen-1].Rindex
		 break
	     }
	     for _,v:=range args.Log[i:]{
		 fmt.Printf("%d app from %d the %d:%d i=%d\n",rf.me,args.LeaderId,v.ArrayIndex,v.Log.Term,i)
		 v.Applied=false
                 rf.logs=append(rf.logs,v)
	     }

	     rf.logLen=len(rf.logs)
	     rf.index=rf.logs[rf.logLen-1].Rindex
             rf.persist()
	     rf.timeAccu=0
     }
 
     var commited int
     if task {
	if args.Commited >= rf.logLen {
	   commited=rf.logLen-1
	}else{
	   commited=args.Commited
	}
     }else{
	if args.Commited >= index{
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
//        fmt.Printf("%d agree %d (%d)append the index:%d term:%d \n",rf.me,args.LeaderId,preStatus,args.Log.ArrayIndex,args.Log.Log.Term)
//        fmt.Print(reply.Repeat)
//	fmt.Printf(" commited now:%d\n args commite %d pre %d:%d next %d:%d\n",rf.commited,args.Commited,args.LastLogIndex,args.LastLogTerm,args.Log.ArrayIndex,args.Log.Log.Term)
}

//在调用make时，作为后台进程调用
func(rf *Raft) Work(){

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


func(rf *Raft) countThread(out *bool){
     for rf.status==candidate {
         rf.ResetTime()
         time.Sleep(time.Millisecond*time.Duration(rf.timeout))
	 rf.mu.Lock()
	 (*out)=true
         rf.cond.Broadcast()
	 rf.mu.Unlock()
     }
}

func(rf *Raft) playCandidate(){

      first:=true
      out:=false
      for rf.status==candidate && !rf.killed(){
	  rf.curTerm=rf.curTerm+1
	  term:=rf.curTerm
	  lastIndex:=rf.logs[rf.logLen-1].ArrayIndex
	  lastTerm:=rf.logs[rf.logLen-1].Log.Term
	  rf.voteFor=rf.me
	  out=false
	  rf.persist()
	  rf.mu.Unlock()
	  win:=1
	  fmt.Printf("%d make a vote of %d\n",rf.me,term)
          if first {
            go rf.countThread(&out)
	    first=false
          }
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
		 go rf.sendRequestVote(i,args,reply,&win,term)
	  }
	  rf.mu.Lock()
	  for win<=rf.mainPeers && rf.status==candidate && !out {
		 rf.cond.Wait()
	  }

          if rf.status!=candidate {
             rf.mu.Unlock()
	     return
	  }

          if win>rf.mainPeers {
	       fmt.Printf("%d become the leader of %d\n",rf.me,rf.curTerm)
	       rf.status=leader
	       rf.leader=rf.me
	       pre:=rf.logLen
	       log:=rf.emptyLog()
	       log.Sindex=pre
	       rf.logs=append(rf.logs,*log)
	       rf.persist()
	       rf.logLen=len(rf.logs)
	       rf.leaIndex=pre
	       for i,_:=range rf.peers {
		      if i==rf.me {
	              continue
		   }
		   rf.nextIndex[i]=pre
	       }
	       rf.mu.Unlock()
	       return
	  }

	  fmt.Printf("%d'vote end in %d\n",rf.me,win)
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
    rf.logs[index].Applied=true
    rf.persist()
    if rf.logs[index].Log.Command!=nil {
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
     var index int
     for rf.status==leader&&!rf.killed(){
        index=rf.nextIndex[wh]
	for index>=rf.logLen && !rf.killed(){//跟随者日志长度和领导者相同则等待新日志产生
            rf.cond.Wait()
	}
        if rf.status!=leader || rf.killed() {
	   rf.mu.Unlock()
	   return
	}
	fmt.Printf("%d-%d index:%d len:%d\n",rf.me,wh,index,rf.logLen)
	curTerm:=rf.curTerm
	commited:=rf.commited
	logs:=rf.logs[index:]
        rf.mu.Unlock()

        args:=new(AppendEntriesArgs)
        reply:=new(AppendEntriesReply)
        args.NextLogIndex=index
        args.LastLogIndex=rf.logs[index-1].ArrayIndex
	args.LastLogTerm=rf.logs[index-1].Log.Term
        args.CurTerm=curTerm
        args.LeaderId=rf.me
	args.Log=logs
        args.Commited=commited
        ok:=rf.peers[wh].Call("Raft.AppendEntries",args,reply)
        
        rf.mu.Lock()

        if rf.status!=leader {
           rf.mu.Unlock()
	   return
	}

        if ok {	
            if reply.Success {
		 length:=len(logs)
                 if rf.matchIndex[wh] < index+length-1{
		     preCommited:=rf.commited
		     for i:=0;i<length;i++ {
			 if index+i <= rf.matchIndex[wh] {
                            continue
			 }
			 rf.matchIndex[wh]=index+i
			 rf.logs[index+i].Count++
			 if (rf.logs[index+i].Count-rf.mainPeers==1)&&(index+i)>=rf.leaIndex {
			   rf.commited=index+i
			   fmt.Printf("%d commited index: %d count:%d\n",rf.me,index+i,rf.logs[index+i].Count)
			 }
		     }
		     if rf.commited > preCommited {
			rf.ccond.Broadcast()
		     }
	        }
                rf.nextIndex[wh]=rf.matchIndex[wh]+1
		fmt.Printf("%d-%d nextIndex:%d\n",rf.me,wh,rf.nextIndex[wh])

	    }else if reply.CurTerm > rf.curTerm {
		rf.curTerm=reply.CurTerm
		rf.status=follower
		rf.leader=-1
		rf.voteFor=-1
		rf.persist()
		rf.mu.Unlock()
		return
	    }else if reply.CurTerm == rf.curTerm && reply.Xindex!=-1 { 
		fmt.Printf("%d-%d reply%d %d %d index:%d\n",rf.me,wh,reply.Xterm,reply.Xindex,reply.Xlen,index)
                if reply.Xlen < index {
		   fmt.Printf("%d-%d reply.Xlen:%d < index:%d\n",rf.me,wh,reply.Xlen,index)
                   rf.nextIndex[wh]=reply.Xlen
		   if reply.Xlen==0 {
		       fmt.Printf("%d recv reply len:0 from %d\n",rf.me,wh)
		   }
		}else{
                   index--
                   for rf.logs[index].Log.Term!=reply.Xterm&&reply.Xterm<rf.logs[index].Log.Term {
                       index=rf.logs[index].Sindex-1    
		   }
                   if rf.logs[index].Log.Term!=reply.Xterm{
                       index=reply.Xindex
		   }
                   if index==0 {
                      fmt.Printf("%d-%d turn to 0 for %d %d %d\n",rf.me,wh,reply.Xterm,reply.Xindex,reply.Xlen)
		   }
                   rf.nextIndex[wh]=index
		}
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
		     for i,_:=range rf.peers {
			 if i==rf.me {
			    continue
			 }
			 go rf.leaderExecu(i)
		     }
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
			args.Log=nil
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply,win *int,term int){
    ok := rf.peers[server].Call("Raft.RequestVote", args, reply)	
    rf.mu.Lock()
    if rf.curTerm!=term||rf.status!=candidate{
       rf.mu.Unlock()
       return
    }
    if ok {
       if reply.Vote {
	  (*win)++
       }else if reply.CurTerm > rf.curTerm {
	   rf.curTerm=reply.CurTerm
	   rf.voteFor=-1
	   rf.status=follower
	   rf.leader=-1
	   rf.persist()
	   rf.cond.Broadcast()
       }
    }
    if (*win)-rf.mainPeers==1{
       rf.cond.Broadcast()
    }
    rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply){

    ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)	
    if ok {	   
      rf.mu.Lock()
      if reply.CurTerm > rf.curTerm {
	   rf.curTerm=reply.CurTerm
	   rf.status=follower
	   rf.leader=-1
	   rf.voteFor=-1
	   rf.persist()
      }
      rf.mu.Unlock()
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
	   log.Sindex=rf.leaIndex
	   rf.logs=append(rf.logs,*log)
	   rf.persist()
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
	rf.ccond.Broadcast()
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

         for (!rf.killed())&&(rf.lastApplied >= rf.commited || rf.logLen <= (rf.lastApplied+1)) {
             rf.ccond.Wait()
	 }
         if rf.killed() { 
	    rf.mmu.Unlock()
	    return
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
     rf.status=follower
     rf.mainPeers=len(peers)/2
    rf.timeBeat=80
    rf.timeBase=220

    /******************2B******************/
    rf.lastApplied=0
    rf.index=1
    log:=rf.emptyLog()
    log.Applied=true
    log.Sindex=0
    rf.logs=append(rf.logs,*log)
    rf.logLen=len(rf.logs)
    rf.msgChan=applyCh
    rf.commited=0 //默认提交了第一条空索引
    rf.nextIndex=make([]int,len(peers))
    rf.ccond=sync.NewCond(&rf.mmu)
    
    rf.matchIndex=make([]int,len(peers))
    for i:=range rf.matchIndex {
        rf.matchIndex[i]=0
    }

    rf.readPersist(persister.ReadRaftState()) 
    for i:=rf.logLen-1;i>=0;i--{
        if rf.logs[i].Applied {
           rf.lastApplied=i
	   break
	}
    }
    go rf.applyThread()
    go rf.Work()
    return rf
}








