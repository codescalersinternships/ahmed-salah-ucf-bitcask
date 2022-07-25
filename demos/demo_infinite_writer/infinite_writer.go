package main

import (
	"bitcask"
	"fmt"
	"path"
)

func main() {
	bc, err := bitcask.Open(path.Join("bitcask"), bitcask.RWsyncConfig)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	for { }

	bc.Close()
}