package client

import (
	"fmt"
	"github.com/sushantsondhi/raft-col733/common"
	"github.com/sushantsondhi/raft-col733/kvstore"
	"strings"
)

// RunCliClient method starts a simple REPL program
// using the kvstore library.
func RunCliClient(servers []common.Server, manager common.RPCManager) error {
	store, err := kvstore.NewKeyValStore(servers, manager)
	if err != nil {
		return err
	}
	fmt.Println("<<<< KV Store Using Raft >>>>")
	fmt.Println("Available commands: ")
	fmt.Println("\t GET <key>")
	fmt.Println("\t SET <key> <val>")
	fmt.Printf("\n\n")
	for {
		fmt.Printf("$ ")
		var command, key, val string
		if _, err := fmt.Scanf("%s", &command); err != nil {
			return err
		}
		switch strings.ToUpper(command) {
		case "GET":
			if _, err := fmt.Scanln(&key); err != nil {
				return err
			}
			_, val, err := store.Get(key)
			if err != nil {
				fmt.Println(err)
			} else {
				fmt.Printf("%s = %s, OK\n", key, val)
			}
		case "SET":
			if _, err := fmt.Scanln(&key, &val); err != nil {
				return err
			}
			_, err := store.Set(key, val)
			if err != nil {
				fmt.Println(err)
			} else {
				fmt.Printf("%s = %s, OK\n", key, val)
			}
		default:
			fmt.Println("Incorrect command")
		}
	}
}
