# Bitcask Description
The origin of Bitcask is tied to the history of the Riak distributed database. In a Riak key/value cluster, each
node uses pluggable local storage; nearly anything k/v-shaped can be used as the per-host storage engine. This
pluggability allowed progress on Riak to be parallelized such that storage engines could be improved and tested
without impact on the rest of the codebase.
**NOTE:** All project specifications and usage are mentioned in this [Official Bitcask Design Paper](https://riak.com/assets/bitcask-intro.pdf)


# Bitcask API

| Function                                                      | Description                                            |
|---------------------------------------------------------------|--------------------------------------------------------|
| ```func Open(directoryPath string, config ...Config) (*Bitcask, error)```| Open a new or an existing bitcask datastore |
| ```func (bc *Bitcask) Put(key []byte, value []byte) error```| Stores a key and a value in the bitcask datastore |
| ```func (bc *Bitcask) Get(key []byte) ([]byte, error)```| Reads a value by key from a datastore |
| ```func (bc *Bitcask) Delete(key []byte) error```| Removes a key from the datastore |
| ```func (bc *Bitcask) Close()```| Close a bitcask data store and flushes all pending writes to disk |
| ```func (bc *Bitcask) ListKeys() [][]byte```| Returns list of all keys |
| ```func (bc *Bitcask) Sync() error```| Force any writes to sync to disk |
| ```func (bc *Bitcask) Merge() error```| Merge several data files within a Bitcask datastore into a more compact form. Also, produce hintfiles for faster startup. |
| ```func (bc *Bitcask) Fold(fun func([]byte, []byte, any) any, acc any) any```| Fold over all K/V pairs in a Bitcask datastore.→ Acc Fun is expected to be of the form: F(K,V,Acc0) → Acc |

-----
## Basic demo_infinite_writer
```go
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
```
#### **NOTE:** there is no output for this infinite writer, it is used to lock the bitcask.
---
## Basic demo_writer
```go
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
```
## output with this only writer
```
$ go run demos/demo_writer/writer.go 
***** append 100 item *****
****** merge old files *******
****** Get some items ********
value of key2 is: value2
value of key15 is: value15
value of key77 is: value77
****** close bitcask *******
```
## output if there another writer
```
$ go run demos/demo_writer/writer.go
there is another process that locked this bitcask
```
---

## Basic demo_infinite_reader
```go
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

	for { }

	bc.Close()
}
```
---
## Basic demo_reader
```go
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
	val26, _ := bc.Get("key26")
	fmt.Printf("value of key26 is: %s\n", string(val26))
	val10, _ := bc.Get("key10")
	fmt.Printf("value of key10 is: %s\n", string(val10))
	val89, _ := bc.Get("key89")
	fmt.Printf("value of key89 is: %s\n", string(val89))
	bc.Close()
}
```
## output if exist bitcask
```
$ go run demos/demo_reader/reader.go
****** Get some items ********
value of key2 is: 
value of key15 is: 
value of key77 is: 
****** close bitcask *******
```

## output if there is writer exist
```
$ go run demos/demo_reader/reader.go
there is another process that locked this bitcask
```

## output if there is reader exist
```
$ go run demos/demo_reader/reader.go
****** Get some items ********
value of key2 is: 
value of key15 is: 
value of key77 is: 
****** close bitcask *******
```