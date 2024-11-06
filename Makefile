build:
	go build -o ./bin/main main.go

run: build
	../ms/maelstrom test -w broadcast --bin ./bin/main --node-count 1 --time-limit 20 --rate 10

multi: build
	../ms/maelstrom test -w broadcast --bin ./bin/main --node-count 5 --time-limit 20 --rate 10

partition: build
	../ms/maelstrom test -w broadcast --bin ./bin/main --node-count 5 --time-limit 20 --rate 10 --nemesis partition
