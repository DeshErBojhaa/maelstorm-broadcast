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
	var (
		values []int
		mu     sync.Mutex
	)

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body Message
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		val := body.Message
		mu.Lock()
		if slices.Contains(values, val) {
			return nil
		}
		values = append(values, val)
		mu.Unlock()
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
		return n.Reply(msg, map[string]any{
			"type":     "read_ok",
			"messages": values,
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
