package main

import "github.com/23jdd/dfs"

func main() {
	s, err := dfs.NewMaster("meta.db")
	if err != nil {
		panic(err)
	}
	s.Run(":8888")
}
