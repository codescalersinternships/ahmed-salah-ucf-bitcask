package main

import (
	"bitcask"
	"fmt"
	"path"
)

func main() {
	bc, err := bitcask.Open(path.Join("bitcask"))
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	fmt.Println("****** Get some items ********")
	val2, _ := bc.Get([]byte("key2"))
	fmt.Printf("value of key2 is: %s\n", string(val2))

	val15, _ := bc.Get([]byte("key15"))
	fmt.Printf("value of key15 is: %s\n", string(val15))

	val77, _ := bc.Get([]byte("key77"))
	fmt.Printf("value of key77 is: %s\n", string(val77))

	fmt.Println("****** close bitcask *******")
	bc.Close()
}