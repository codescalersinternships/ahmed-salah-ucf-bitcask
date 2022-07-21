package bitcask

import (
	"fmt"
	"os"
	"path"
	"testing"
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
        fmt.Fprintln(testKeyDir, "key 1 50 0 3")

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
        os.MkdirAll(path.Join("no open dir"), 000)
        _, err := Open("no open dir")
        if err == nil {
            t.Fatal("expected Error since path cannot be openned")
        }
    })
}

func TestGet(t *testing.T) {
	t.Run("key is nil", func(t *testing.T) {
		bc, _ := Open(testBitcaskPath)
		_, err := bc.Get(nil)

		assertErrorMsg(t, err, ErrNullKey)
	})

	t.Run("data in pending writes", func(t *testing.T) {
		bc, _ := Open(testBitcaskPath, syncConfig)
		pendingWrites["name"] = []byte("salah")
		got, _ := bc.Get([]byte("name"))
		want := "salah"

		assertEqualStrings(t, string(got), want)
	})

	/* To-Do:  "Data from file" test
			   "Invalid file id" testr
	  		   "Read files less than item size" test
	*/
}


func assertEqualStrings(t testing.TB, got, want string) {
	t.Helper()
	if (got != want) {
		t.Errorf("got %s want %s", got, want)
	}
}

func assertErrorMsg(t testing.TB, err, want error) {
	t.Helper()
	if err == nil {
		t.Fatal("didn't get error, expected to get an error")
	}

	if err.Error() != want.Error() {
		t.Errorf("got %q want %q", err.Error(), want)
	}
}