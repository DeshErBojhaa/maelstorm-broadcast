package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"slices"
	"sync"
	"time"
)

type Topo struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology"`
}

type Message struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
}

func main() {
	n := maelstrom.NewNode()
	graph := make(map[string][]string)
	values := make(map[int]struct{})
	mu := sync.RWMutex{}

	go func() {
		tk := time.NewTicker(time.Second)
		defer tk.Stop()
		for {
			select {
			case <-tk.C:
				retry := make([]int, 0, len(values))
				mu.RLock()
				for v := range values {
					retry = append(retry, v)
				}
				mu.RUnlock()
				for v := range retry {
					for _, nxt := range graph[n.ID()] {
						if nxt == n.ID() {
							continue
						}
						_ = n.RPC(nxt, Message{Type: "broadcast", Message: v}, func(_ maelstrom.Message) error {
							return nil
						})
					}
				}
			}
		}
	}()

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body Message
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		val := body.Message
		mu.Lock()
		_, ok := values[val]
		if !ok {
			values[val] = struct{}{}
		}
		mu.Unlock()
		if ok {
			return nil
		}
		for _, nxt := range graph[n.ID()] {
			if nxt == n.ID() {
				continue
			}
			if err := n.RPC(nxt, body, func(_ maelstrom.Message) error {
				return nil
			}); err != nil {
				return err
			}
		}
		return n.Reply(msg, map[string]string{"type": "broadcast_ok"})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body Topo
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		graph = body.Topology
		return n.Reply(msg, map[string]string{"type": "topology_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		val := make([]int, 0, len(values))
		mu.RLock()
		for v := range values {
			val = append(val, v)
		}
		mu.RUnlock()
		slices.Sort(val)
		return n.Reply(msg, map[string]any{
			"type":     "read_ok",
			"messages": val,
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
