package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"slices"
	"sync"
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
	failed := make(map[int]struct{})
	mu := sync.RWMutex{}

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body Message
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		val := body.Message
		mu.Lock()
		if _, ok := values[val]; ok {
			return nil
		}
		values[val] = struct{}{}
		mu.Unlock()
		for _, nxt := range graph[n.ID()] {
			if nxt == n.ID() {
				continue
			}
			if err := n.RPC(nxt, body, func(_ maelstrom.Message) error {
				return nil
			}); err != nil {
				failed[val] = struct{}{}
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
