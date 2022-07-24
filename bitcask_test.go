package bitcask

import (
	"fmt"
	"os"
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
        t.Run(tt.testName, func(t *testing.T) {
            bc, _ := Open(testBitcaskPath, RWsyncConfig)
            err := bc.Put(tt.key, tt.value)

            assertErrorMsg(t, err, tt.testErr)
            os.RemoveAll(testBitcaskPath)
        })
    }

    t.Run("has no write permissions", func(t *testing.T) {
        bc, _ := Open(testBitcaskPath)
        err := bc.Put([]byte("key"), []byte("value"))

        assertErrorMsg(t, err, ErrHasNoWritePerms)
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
