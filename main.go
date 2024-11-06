package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
)

type Topo struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology"`
}

func main() {
	n := maelstrom.NewNode()
	graph := make(map[string][]string)
	var values []int

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		val := int(body["message"].(float64))
		values = append(values, val)
		for _, nxt := range graph[n.ID()] {
			if err := n.Send(nxt, val); err != nil {
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
