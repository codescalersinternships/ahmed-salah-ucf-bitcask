package bitcask

import (
	"fmt"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func TestOpen(t *testing.T) {
	t.Run("new bitcask with default config given implicit", func(t *testing.T) {
		Open(testBitcaskPath)

		if _, err := os.Stat(testBitcaskPath); os.IsNotExist(err) {
		    t.Errorf("expected to find directory: %q", testBitcaskPath)
		}
		os.RemoveAll(testBitcaskPath)
	})

	t.Run("new bitcask with default config given explict", func(t *testing.T) {
		Open(testBitcaskPath, DefaultConfig)

		if _, err := os.Stat(testBitcaskPath); os.IsNotExist(err) {
		    t.Errorf("expected to find directory: %q", testBitcaskPath)
		}
		os.RemoveAll(testBitcaskPath)
	})

	t.Run("new bitcask with read and write permission", func(t *testing.T) {
		Open(testBitcaskPath, RWConfig)

		if _, err := os.Stat(testBitcaskPath); os.IsNotExist(err) {
		    t.Errorf("expected to find directory: %q", testBitcaskPath)
		}
		os.RemoveAll(testBitcaskPath)
	})

	t.Run("new bitcask with sync permission", func(t *testing.T) {
		Open(testBitcaskPath, syncConfig)

		if _, err := os.Stat(testBitcaskPath); os.IsNotExist(err) {
		    t.Errorf("expected to find directory: %q", testBitcaskPath)
		}
		os.RemoveAll(testBitcaskPath)
	})

	t.Run("existing bitcask with read and write permission", func(t *testing.T) {
		Open(testBitcaskPath, RWConfig)

		testKeyDir, _ := os.Create(testKeyDirPath)
		fmt.Fprintln(testKeyDir, "key 1 50 0 3")

		Open(testBitcaskPath, RWConfig)

		if _, err := os.Stat(testBitcaskPath); os.IsNotExist(err) {
		    t.Errorf("expected to find directory: %q", testBitcaskPath)
		}
		os.RemoveAll(testBitcaskPath)
	})

	t.Run("existing bitcask with sync permission", func(t *testing.T) {
		Open(testBitcaskPath, syncConfig)

        testKeyDir, _ := os.Create(testKeyDirPath)
        fmt.Fprintln(testKeyDir, "key 1 20 2 3")

		Open(testBitcaskPath, syncConfig)

		if _, err := os.Stat(testBitcaskPath); os.IsNotExist(err) {
		    t.Errorf("expected to find directory: %q", testBitcaskPath)
		}

		os.RemoveAll(testBitcaskPath)
	})

	t.Run("existing bitcask with default options", func(t *testing.T) {
		Open(testBitcaskPath)

		testKeyDir, _ := os.Create(testKeyDirPath)
		fmt.Fprintln(testKeyDir, "key 1 50 0 3")

		Open(testBitcaskPath)

		if _, err := os.Stat(testBitcaskPath); os.IsNotExist(err) {
		    t.Errorf("expected to find directory: %q", testBitcaskPath)
		}

		os.RemoveAll(testBitcaskPath)
	})

	t.Run("bitcask has no permissions", func(t *testing.T) {
        os.MkdirAll(testNoOpenDirPath, NoPermissions)
        _, err := Open(testNoOpenDirPath)
        if err == nil {
            t.Fatal("expected Error since path cannot be openned")
        }

		os.RemoveAll(testNoOpenDirPath)
	})

    /* To-Do:  "concurrent process load exist keydir" test
		
	*/
}

func TestGet(t *testing.T) {
	t.Run("key is nil", func(t *testing.T) {
        bc, _ := Open(testBitcaskPath)
        _, err := bc.Get(nil)

        assertErrorMsg(t, err, ErrNullKeyOrValue)
        os.RemoveAll(testBitcaskPath)
	})

    t.Run("key doesn't exist", func(t *testing.T) {
		bc, _ := Open(testBitcaskPath)
		_, err := bc.Get([]byte("unknown key"))
		want := BitCaskError("\"unknown key\": key doesn't exist")

		assertErrorMsg(t, err, want)
		os.RemoveAll(testBitcaskPath)
    })

	t.Run("data in pending writes", func(t *testing.T) {
        bc, _ := Open(testBitcaskPath, syncConfig)
        bc.keydir["name"] = Record{}
        pendingWrites["name"] = []byte("salah")
        got, _ := bc.Get([]byte("name"))
        want := "salah"

        assertEqualStrings(t, string(got), want)
        os.RemoveAll(testBitcaskPath)
	})

    t.Run("existing value from file", func(t *testing.T) {
        os.MkdirAll(testBitcaskPath, UserReadWriteExec)
        file, _ := os.Create(testFilePath)

        bc, _ := Open(testBitcaskPath)
        file.Write(bc.makeItem([]byte("key"), []byte("value"), time.Now()))


        bc.keydir["key"] = Record {
            fileId:  testFilePath,
            valueSize: len("value"),
            valuePosition:  int64(16 + len("key")),
            timeStamp:  time.Now(),
        }

        got, _ := bc.Get([]byte("key"))
        want := "value"

        assertEqualStrings(t, string(got), want)
        os.RemoveAll(testBitcaskPath)
    })

    t.Run("read number of bytes less than size of value", func(t *testing.T) {
        os.MkdirAll(testBitcaskPath, UserReadWriteExec)
        file, _ := os.Create(testFilePath)

        bc, _ := Open(testBitcaskPath)
        file.Write(bc.makeItem([]byte("key"), []byte("value"), time.Now()))


        bc.keydir["key"] = Record {
            fileId:  testFilePath,
            valueSize: 8,   // invalid value size
            valuePosition:  int64(16 + len("key")),
            timeStamp:  time.Now(),
        }

        _, err := bc.Get([]byte("key"))
        want := fmt.Errorf("read only 5 bytes out of 8")

        assertErrorMsg(t, err, want)
        os.RemoveAll(testBitcaskPath)
    })

    t.Run("invalid file id", func(t *testing.T) {
        os.MkdirAll(testBitcaskPath, 0700)
        file, _ := os.Create(testFilePath)

        bc, _ := Open(testBitcaskPath)
        file.Write(bc.makeItem([]byte("key"), []byte("value"), time.Now()))


        bc.keydir["key"] = Record {
            fileId:  "invalid file id",
            valueSize: len("value"),
            valuePosition:  int64(16 + len("key")),
            timeStamp:  time.Now(),
        }

        _, err := bc.Get([]byte("key"))
        want := BitCaskError("can't open file: invalid file id")

        assertErrorMsg(t, err, want)
        os.RemoveAll(testBitcaskPath)
    })
}

func TestPut(t *testing.T) {
    t.Run("has no write permissions", func(t *testing.T) {
        bc, _ := Open(testBitcaskPath)
        err := bc.Put([]byte("key"), []byte("value"))

        assertErrorMsg(t, err, ErrHasNoWritePerms)
        os.RemoveAll(testBitcaskPath)
    })

    t.Run("successful put", func(t *testing.T) {
        bc, _ := Open(testBitcaskPath, RWsyncConfig)
        bc.Put([]byte("name"), []byte("salah"))
        
        got, _ := bc.Get([]byte("name"))

        assertEqualStrings(t, string(got), string("salah"))
        os.RemoveAll(testBitcaskPath)
    })

    t.Run("syncOnPut disabled", func(t *testing.T) {
        bc, _ := Open(testBitcaskPath, RWConfig)
        bc.Put([]byte("name"), []byte("salah"))
        
        got, _ := bc.Get([]byte("name"))

        assertEqualStrings(t, string(got), string("salah"))
        os.RemoveAll(testBitcaskPath)
    })

    t.Run("syncOnPut disabled and key exist in pendings", func(t *testing.T) {
        bc, _ := Open(testBitcaskPath, RWConfig)
        bc.Put([]byte("name"), []byte("salah"))

        bc.Put([]byte("name"), []byte("ahmed"))
        
        got, _ := bc.Get([]byte("name"))

        assertEqualStrings(t, string(got), string("ahmed"))
        os.RemoveAll(testBitcaskPath)
    })

    var tests = [] struct {
        testName string
        key []byte
        value []byte
        testErr error

    } {
        {"nil key", nil, []byte("salah"), ErrNullKeyOrValue},
        {"nil value", []byte("name"), nil, ErrNullKeyOrValue},
    }
    for _, tt := range tests {
        os.RemoveAll(testBitcaskPath)
        t.Run(tt.testName, func(t *testing.T) {
            bc, _ := Open(testBitcaskPath, RWsyncConfig)
            err := bc.Put(tt.key, tt.value)

            assertErrorMsg(t, err, tt.testErr)
            os.RemoveAll(testBitcaskPath)
        })
    }

    // var passMaxSizeTests = [] struct {
    //     testName string
    //     config Config
    // } {
    //     {"pass MaxFileSize", RWsyncConfig},
    //     {"pass MaxPendingSize", RWConfig},
    // }
    // for _, tt := range passMaxSizeTests {
    //     t.Run(tt.testName, func(t *testing.T) {
    //         os.RemoveAll(testBitcaskPath)
    //         bc, _ := Open(testBitcaskPath, tt.config)
    //         for i := 0; i < 100; i++ {
    //             key := "key" + fmt.Sprintf("%d", i)
    //             value := "value" + fmt.Sprintf("%d", i)
    //             bc.Put([]byte(key), []byte(value))
    //         }
            
    //         got, _ := bc.Get([]byte("key5"))
            
    //         assertEqualStrings(t, string(got), "value5")
    //         os.RemoveAll(testBitcaskPath)
    //     })
    // }
}

func TestDelete(t *testing.T) {
    t.Run("passed nil key", func(t *testing.T) {
        bc, _ := Open(testBitcaskPath, RWsyncConfig)
            err := bc.Delete(nil)

            assertErrorMsg(t, err, ErrNullKeyOrValue)
            os.RemoveAll(testBitcaskPath)
    })

    t.Run("has no write permissions", func(t *testing.T) {
        bc, _ := Open(testBitcaskPath)
        err := bc.Delete([]byte("key"))

        assertErrorMsg(t, err, ErrHasNoWritePerms)
        os.RemoveAll(testBitcaskPath)
    })

    t.Run("delete with syncOnPut enabled", func(t *testing.T) {
        bc, _ := Open(testBitcaskPath, RWsyncConfig)
        bc.Put([]byte("key"), []byte("value"))
        
        bc.Delete([]byte("key"))

        _, err := bc.Get([]byte("key"))
		want := BitCaskError("\"key\": key doesn't exist")

		assertErrorMsg(t, err, want)
		os.RemoveAll(testBitcaskPath)
    })

    t.Run("delete with syncOnPut disabled", func(t *testing.T) {
        bc, _ := Open(testBitcaskPath, RWConfig)
        bc.Put([]byte("key"), []byte("value"))
        
        bc.Delete([]byte("key"))

        _, err := bc.Get([]byte("key"))
		want := BitCaskError("\"key\": key doesn't exist")

		assertErrorMsg(t, err, want)
		os.RemoveAll(testBitcaskPath)
    })
}

func TestListKeys(t *testing.T) {
    t.Run("empty bitcask", func(t *testing.T) {
        bc, _ := Open(tetsListKeyBitcaskPath, RWsyncConfig)
        got := bc.ListKeys()

        if len(got) != 0 {
            t.Errorf("length of keys list is %d, expected to get 0", len(got))
        }
        os.RemoveAll(tetsListKeyBitcaskPath)
    })

    t.Run("list keys succesfully", func(t *testing.T) {
        bc, _ := Open(tetsListKeyBitcaskPath, RWConfig)
        bc.Put([]byte("name"), []byte("salah"))

        got := bc.ListKeys()
        want := [][]byte {[]byte("name")}

        if !reflect.DeepEqual(got, want) {
            t.Errorf("got:\n%v\nwant:\n%v", got, want)
        }
        os.RemoveAll(tetsListKeyBitcaskPath)
    })
}

func TestFold(t *testing.T) {
    bc, _ := Open(testBitcaskPath, RWsyncConfig)
    bc.Put([]byte("key1"), []byte("1"))
    bc.Put([]byte("key2"), []byte("2"))
    bc.Put([]byte("key3"), []byte("3"))

    sum := func(key, value []byte, acc any) any {
        val, _ := strconv.Atoi(string(value))

        return val + acc.(int)
    }

    got := bc.Fold(sum, 0)
    want := 6

    if got != want {
        t.Errorf("got:\n%d\nwant:\n%d\n", got, want)
    }

    os.RemoveAll(testBitcaskPath)
}

func TestMerge(t *testing.T) {
    t.Run("has no write permissions", func(t *testing.T) {
        bc , _ := Open(testBitcaskPath)
        err := bc.Merge()

        assertErrorMsg(t, err, ErrHasNoWritePerms)
        os.RemoveAll(testBitcaskPath)
    })

    var tests = [] struct {
        testName string
        config Config
    } {
        {"merge files successfully", RWsyncConfig},
        {"merge files with pendding writes", RWConfig},
    }
    for _, tt := range tests {
        t.Run(tt.testName, func(t *testing.T) {
            os.RemoveAll(testBitcaskMergePath)
            bc, _ := Open(testBitcaskMergePath, tt.config)
            for i := 0; i < 100; i++ {
                key := "key" + fmt.Sprintf("%d", i)
                value := "value" + fmt.Sprintf("%d", i)
                bc.Put([]byte(key), []byte(value))
            }
            
            bc.Merge()
            got, _ := bc.Get([]byte("key5"))
            
            assertEqualStrings(t, string(got), "value5")
            os.RemoveAll(testBitcaskMergePath)
        })
    }
}

func TestSync(t *testing.T) {
    t.Run("has no write permissions", func(t *testing.T) {
        bc, _ := Open(testBitcaskPath)
        err := bc.Sync()

        assertErrorMsg(t, err, ErrHasNoWritePerms)
        os.RemoveAll(testBitcaskPath)
    })

    t.Run("flush pending writes successfuly", func(t *testing.T) {
        bc, _ := Open(testBitcaskPath, RWConfig)
        bc.Put([]byte("name"), []byte("salah"))
        bc.Sync()
        got, _ := bc.Get([]byte("name"))

        assertEqualStrings(t, string(got), string("salah"))
        os.RemoveAll(testBitcaskPath)
    })
}



func assertEqualStrings(t testing.TB, got, want string) {
	t.Helper()
	if (got != want) {
		t.Errorf("got:\n%q\nwant:\n%q", got, want)
	}
}

func assertErrorMsg(t testing.TB, err, want error) {
	t.Helper()
	if err == nil {
		t.Fatalf("didn't get error, expected to get an error %q", want.Error())
	}

	if err.Error() != want.Error() {
		t.Errorf("got:\n%q\nwant:\n%q", err.Error(), want)
	}
}
