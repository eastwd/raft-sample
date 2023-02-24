package main

//https://hazm.at/mox/distributed-system/algorithm/transaction/raft/index.html#leader-election

import (
	"fmt"
	"math"
	"math/rand"
	"time"
)

var Nodes map[string]*Node

type Node struct {
	Id    string
	State State

	//永続データ
	Term  int
	Voted bool
	Log   []Log

	//揮発性データ
	CommitIndex int
	LastApplied int

	//Leader用の揮発性データ
	NextIndex  map[string]int
	MatchIndex map[string]int

	//データ送受信用のソケットの代わり
	VReq  chan VoteRequest
	VRes  chan VoteResponse
	AEReq chan AppendEntriesRequest
	AERes chan AppendEntriesResponse

	//Leader用のデータ送受信用のソケットの代わり
	LogReq chan NewLogRequest
}

// クライアントから送信されるデータ
type NewLogRequest struct {
	Value int
}

type Log struct {
	Term  int
	Value int
}

type State string

const (
	Follower  State = "follower"
	Candidate State = "candidate"
	Leader    State = "leader"
)

func NewNode(id string) *Node {
	return &Node{
		State:       Follower,
		Term:        0,
		Id:          id,
		Voted:       false,
		CommitIndex: 0,
		LastApplied: 0,
		Log:         []Log{},
		AEReq:       make(chan AppendEntriesRequest, 10),
		AERes:       make(chan AppendEntriesResponse, 10),
		VReq:        make(chan VoteRequest, 10),
		VRes:        make(chan VoteResponse, 10),
		LogReq:      make(chan NewLogRequest, 10),
		MatchIndex:  map[string]int{},
		NextIndex:   map[string]int{},
	}
}

func main() {
	Nodes = map[string]*Node{}
	//3ノードで実行
	for i := 0; i < 3; i++ {
		nodeId := fmt.Sprintf("Server%d", i)
		n := NewNode(nodeId)
		Nodes[nodeId] = n
		go n.Run()
	}
	for {
		//定期的にLeaderにデータを送信する
		time.Sleep(10 * time.Second)
		for _, n := range Nodes {
			if n.State == Leader {
				n.LogReq <- NewLogRequest{Value: rand.Int()}
			}
		}
	}
}

func (n *Node) Run() {
	for {
		switch n.State {
		case Follower:
			n.FollowerRun()
		case Candidate:
			n.CandidateRun()
		case Leader:
			n.LeaderRun()
		}
	}
}

func (n *Node) FollowerRun() {

	//Split Voteを防ぐためにelection timeioutには揺らぎを持たせる
	d := time.Duration(rand.Intn(20)+10) * time.Second
	timer := time.NewTimer(d)

	nodelog(*n, "I am follower")
loop:
	for {
		select {
		case r := <-n.AEReq:
			//AppendEntriesRequestを受け取ったらタイマーをリセット
			nodelog(*n, "receive AppendEntriesRequest")
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(d)
			//自分よりTermが低い場合はFalse
			if r.Term < n.Term {
				nodelog(*n, "term is too low")
				Nodes[r.LeaderId].AERes <- AppendEntriesResponse{Term: n.Term, Success: false, MatchIndex: n.LastApplied}
				continue
			}
			//ログがない場合はスキップ
			if n.LastApplied > 0 {
				//PrevLogIndex、PrevLogTermが一致するログがない場合はfalse
				if n.LastApplied+len(r.Logs) < r.PrevLogIndex {
					nodelog(*n, fmt.Sprintf("index is too low: lastApplied=%d, len(r.Logs)=%d, r.PrevLogIndex=%d", n.LastApplied, len(r.Logs), r.PrevLogIndex))
					Nodes[r.LeaderId].AERes <- AppendEntriesResponse{Term: n.Term, Success: false, MatchIndex: n.LastApplied}
					continue
				}
				if n.GetLog(r.PrevLogIndex).Term != r.PrevLogTerm {
					nodelog(*n, "log term unmatch")
					Nodes[r.LeaderId].AERes <- AppendEntriesResponse{Term: n.Term, Success: false, MatchIndex: n.LastApplied}
					continue
				}
			}
			n.Log = append(n.Log, r.Logs...)
			n.LastApplied += len(r.Logs)
			//自分のCommitIndex更新
			if r.LeaderCommit > n.CommitIndex {
				n.CommitIndex = int(math.Min(float64(r.LeaderCommit), float64(len(n.Log)+1)))
				nodelog(*n, fmt.Sprintf("commit index=%d", n.CommitIndex))
			}
			n.Term = r.Term
			Nodes[r.LeaderId].AERes <- AppendEntriesResponse{Term: n.Term, Success: true, MatchIndex: n.LastApplied}
		case r := <-n.VReq:
			nodelog(*n, "receive VoteRequest")
			//自分よりTermが低いか投票済みの場合はFalse
			if r.Term < n.Term || n.Voted {
				Nodes[r.CandidateId].VRes <- VoteResponse{Term: n.Term, vote: false}
				continue
			}
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(d)
			n.Term = r.Term
			n.Voted = true
			Nodes[r.CandidateId].VRes <- VoteResponse{Term: n.Term, vote: true}
		case <-timer.C:
			//Election timeoutした場合はCandidateになる
			nodelog(*n, "election timeout")
			nodelog(*n, "become Candidate")
			n.State = Candidate
			n.Term++
			break loop
		}
	}
}

type VoteRequest struct {
	Term         int
	CandidateId  string
	LastLogIndex int
	LastLogTerm  int
}
type VoteResponse struct {
	Term int
	vote bool
}

func (n *Node) CandidateRun() {
	d := time.Duration(rand.Intn(4)+10) * time.Second
	timer := time.NewTimer(d)
	nodelog(*n, "I am candidate")
loop:
	for {
		select {
		case r := <-n.AEReq:
			nodelog(*n, "receive AppendEntriesRequest")

			//AppendEntriesRequestを受け取った時、Termが自分より高ければFollowerになる
			if r.Term >= n.Term {
				Nodes[r.LeaderId].AERes <- AppendEntriesResponse{
					Term:       n.Term,
					Success:    true,
					MatchIndex: n.LastApplied,
				}
				n.State = Follower
				n.Voted = true
				n.Term = r.Term
				if !timer.Stop() {
					<-timer.C
				}
				break loop
			}

			Nodes[r.LeaderId].AERes <- AppendEntriesResponse{
				Term:       n.Term,
				Success:    false,
				MatchIndex: n.LastApplied,
			}
		case r := <-n.VReq:
			nodelog(*n, "receive VoteRequest")
			//自分よりTermが高い場合はFollowerになる
			if r.Term > n.Term {
				Nodes[r.CandidateId].VRes <- VoteResponse{
					Term: n.Term,
					vote: true,
				}
				n.State = Follower
				n.Voted = true
				n.Term = r.Term
				if !timer.Stop() {
					<-timer.C
				}
				break loop
			}

			//VoteRequestを受け取ったら拒否する
			n.VRes <- VoteResponse{Term: n.Term, vote: false}
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(d)

		case <-timer.C:
			//一定時間Leaderになれない場合はTermをインクリメント
			n.Term++
			break loop

		default:
			//自分自身に投票する
			count := 1
			n.Voted = true

			//隣接ノードにVoteRequestを送る
			for _, node := range Nodes {
				if node.Id == n.Id {
					continue
				}
				nodelog(*n, fmt.Sprintf("send VoteRequest to %s", node.Id))
				node.VReq <- VoteRequest{Term: n.Term, CandidateId: n.Id}
				res := <-n.VRes
				if res.vote {
					count++
				}
			}
			nodelog(*n, fmt.Sprintf("count=%d", count))

			//過半数の賛成が得られればLeaderになる
			if float64(count) > float64(len(Nodes))/2 {
				n.State = Leader
				for _, node := range Nodes {
					if node.Id == n.Id {
						continue
					}
					//Leader用のデータを初期化
					n.MatchIndex[node.Id] = 0
					n.NextIndex[node.Id] = 1
				}
				break loop
			}
		}
	}
}

type AppendEntriesRequest struct {
	Term         int
	LeaderId     string
	Logs         []Log
	LeaderCommit int
	PrevLogIndex int
	PrevLogTerm  int
}

type AppendEntriesResponse struct {
	Term       int
	Success    bool
	MatchIndex int
}

func (n *Node) LeaderRun() {
	nodelog(*n, "I am leader")
loop:
	for {
		select {
		case r := <-n.AEReq:
			nodelog(*n, "receive AppendEntriesRequest")
			//自分より高いTermのAppendEntriesRequestを受け取った時にはFollowerになる
			if r.Term > n.Term {
				Nodes[r.LeaderId].AERes <- AppendEntriesResponse{
					Term:    n.Term,
					Success: true,
				}
				n.State = Follower
				n.Voted = false
				n.Term = r.Term
				break loop
			}
			//それ以外はFalseを返す
			Nodes[r.LeaderId].AERes <- AppendEntriesResponse{
				Term:    n.Term,
				Success: false,
			}
		case req := <-n.LogReq:
			n.Log = append(n.Log, Log{Term: n.Term, Value: req.Value})
			n.LastApplied++
			//本来はここで同期的に隣接ノードにAppendEntriesRequestを送信し、CommitIndexが進んだらレスポンスを返す
			nodelog(*n, "receive a new data from user client")
		default:
			//隣接ノードにログを送信する
			for _, node := range Nodes {
				if node.Id == n.Id {
					continue
				}

				nodelog(*n, fmt.Sprintf("send AppendEntriesRequest to %s", node.Id))
				//送信できていないログがある場合
				if n.NextIndex[node.Id] <= n.LastApplied {
					//最初のログ送信はPrevLogがないのでPrevLogIndexを0で送信
					if n.NextIndex[node.Id] == 1 {
						node.AEReq <- AppendEntriesRequest{
							Term:         n.Term,
							LeaderId:     n.Id,
							LeaderCommit: n.CommitIndex,
							Logs:         n.Log,
							PrevLogTerm:  n.Term,
							PrevLogIndex: 0,
						}
					} else {
						node.AEReq <- AppendEntriesRequest{
							Term:         n.Term,
							LeaderId:     n.Id,
							LeaderCommit: n.CommitIndex,
							Logs:         n.GetLogsFrom(n.NextIndex[node.Id]),
							PrevLogTerm:  n.GetLog(n.MatchIndex[node.Id]).Term,
							PrevLogIndex: n.MatchIndex[node.Id],
						}
					}
				} else {
					//全てのログが送信済みの場合はLogsを空で送る
					node.AEReq <- AppendEntriesRequest{
						Term:         n.Term,
						LeaderId:     n.Id,
						LeaderCommit: n.CommitIndex,
						Logs:         []Log{},
						PrevLogTerm:  n.Term,
						PrevLogIndex: n.MatchIndex[node.Id],
					}
				}
				res := <-n.AERes
				//Trueが返ってきたらそのノード分のIndexデータを更新
				if res.Success {
					n.MatchIndex[node.Id] = res.MatchIndex
					n.NextIndex[node.Id] = res.MatchIndex + 1
				}
			}

			//半数以上のFollowerからtrueが返ってきたらそのIndexまでcommit
			counter := map[int]int{}
			for _, mi := range n.MatchIndex {
				counter[mi]++
			}
			for i, v := range counter {
				if float64(v) > float64(len(Nodes))/2 && i > n.CommitIndex {
					nodelog(*n, fmt.Sprintf("commit index=%d", i))
					n.CommitIndex = i
				}
			}

			//election timeoutの10分の1くらいの頻度でAppendEntriesReuqestを送信する
			time.Sleep(2 * time.Second)
		}
	}
}

func nodelog(n Node, message string) {
	fmt.Printf("[%s:%d:%d:%d][%s]%s\n", n.Id, n.Term, n.CommitIndex, n.LastApplied, n.State, message)
}

func (n *Node) GetLog(index int) Log {
	return n.Log[index-1]
}

func (n *Node) GetLogsFrom(from int) []Log {
	return n.Log[from-1:]
}
