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

	fmt.Println("***** append 100 item *****")
	for i := 0; i < 100; i++ {
		key := "key" + fmt.Sprintf("%d", i)
		value := "value" + fmt.Sprintf("%d", i)
		bc.Put([]byte(key), []byte(value))
	}

	fmt.Println("****** merge old files *******")
	bc.Merge()

	fmt.Println("****** Get some items ********")

	val2, err := bc.Get([]byte("key2"))
	fmt.Printf("value of key2 is: %s\n", string(val2))
	if err != nil {
		fmt.Println(err)
	}

	val15, err := bc.Get([]byte("key15"))
	fmt.Printf("value of key15 is: %s\n", string(val15))
	if err != nil {
		fmt.Println(err)
	}

	val77, err := bc.Get([]byte("key77"))
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("value of key77 is: %s\n", string(val77))

	fmt.Println("****** close bitcask *******")
	bc.Close()
}