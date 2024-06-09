package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/liznear/gopaxos"
)

type Flags struct {
	Port  int
	ID    int
	Peers string
	Debug bool
}

func main() {
	f := Flags{}
	flag.IntVar(&f.Port, "port", 0, "port to listen")
	flag.IntVar(&f.ID, "id", 0, "node id")
	flag.StringVar(&f.Peers, "peers", "", "peers")
	flag.BoolVar(&f.Debug, "debug", false, "debug mode")
	flag.Parse()

	opts := []gopaxos.StateMachineOption{
		gopaxos.WithID(gopaxos.NodeID(f.ID)),
		gopaxos.WithMaxNodes(10),
		gopaxos.WithCommitInterval(time.Second),
	}
	if f.Debug {
		opts = append(opts, gopaxos.WithDebugMode())
	}
	for _, p := range strings.Split(f.Peers, ",") {
		ps := strings.SplitN(p, "=", 2)
		id, err := strconv.Atoi(ps[0])
		if err != nil {
			panic(fmt.Errorf("invalid peer id %q: %w", ps[0], err))
		}
		opts = append(opts, gopaxos.WithNode(gopaxos.NodeID(id), ps[1]))
	}

	kvs := make(map[string]string)
	opts = append(opts, gopaxos.WithExecutor(gopaxos.ExecutorFn(func(ctx context.Context, value []byte) error {
		slog.InfoContext(ctx, "Executing", "req", string(value))
		vs := string(value)
		ps := strings.SplitN(vs, "=", 2)
		kvs[ps[0]] = ps[1]
		return nil
	})))

	sm, err := gopaxos.NewStateMachine(opts...)
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = sm.Start(ctx)
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			line := scanner.Text()
			if line == "exit" {
				cancel()
				return
			}
			if !strings.Contains(line, "=") {
				fmt.Printf("%s=%s\n", line, kvs[line])
				continue
			}
			func() {
				cc, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := sm.Propose(cc, []byte(line)); err != nil {
					slog.Error("Fail to propose: %v", err)
				}
			}()
		}
	}()
	slog.Info("Started running")
	if err := sm.Wait(); err != nil {
		panic(err)
	}
}
