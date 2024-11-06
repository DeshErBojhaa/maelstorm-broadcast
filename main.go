package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"slices"
	"sync"
	"time"
)

type Message struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
}

type Batch struct {
	Type    string `json:"type"`
	Message []int  `json:"message"`
}

func main() {
	n := maelstrom.NewNode()
	graph := make(map[string][]string)
	values := make(map[int]struct{})
	mu := sync.RWMutex{}

	go func() {
		tk := time.NewTicker(time.Second)
		defer tk.Stop()
	Loop:
		for {
			select {
			case <-tk.C:
				retry := make([]int, 0, len(values))
				mu.RLock()
				for v := range values {
					retry = append(retry, v)
				}
				mu.RUnlock()
				if len(retry) == 0 {
					continue Loop
				}
				for _, nxt := range graph[n.ID()] {
					if nxt == n.ID() {
						continue
					}
					_ = n.RPC(nxt, Batch{Type: "batch", Message: retry}, func(_ maelstrom.Message) error {
						return nil
					})
				}

			}
		}
	}()

	n.Handle("batch", func(msg maelstrom.Message) error {
		var body Batch
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		vals := body.Message
		newVals := make([]int, 0)
		mu.Lock()
		for _, val := range vals {
			if _, ok := values[val]; !ok {
				newVals = append(newVals, val)
				values[val] = struct{}{}
			}
		}
		mu.Unlock()

		if len(newVals) == 0 {
			return nil
		}

		body.Message = newVals
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

		return nil
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body Message
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		val := body.Message
		vals := make([]int, 0, len(values))
		mu.Lock()
		_, ok := values[val]
		if !ok {
			values[val] = struct{}{}
		}
		for v := range values {
			vals = append(vals, v)
		}
		mu.Unlock()
		if ok {
			return nil
		}
		for _, nxt := range graph[n.ID()] {
			if nxt == n.ID() {
				continue
			}
			if err := n.RPC(nxt, Batch{
				Type:    "batch",
				Message: vals,
			}, func(_ maelstrom.Message) error {
				return nil
			}); err != nil {
				return err
			}
		}
		return n.Reply(msg, map[string]string{"type": "broadcast_ok"})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		ids := n.NodeIDs()
		nIDs := make(map[string]int, len(ids))
		for i, id := range ids {
			nIDs[id] = i
		}

		for i := range 1 {
			idx := (i + nIDs[n.ID()] + 1) % len(ids)
			graph[n.ID()] = append(graph[n.ID()], ids[idx])
		}
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
