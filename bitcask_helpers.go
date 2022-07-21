package bitcask

import (
	"bufio"
	"fmt"
	"os"
	"path"
	"strings"
	"time"
	"strconv"
)

func parseKeydirData(keydirData string) (Keydir, error) {
	var keydir Keydir = Keydir{}
	var vSz int
	var vPos int
	var t time.Time
	var err error
	scanner := bufio.NewScanner(strings.NewReader(keydirData))

	for scanner.Scan() {
		line := scanner.Text()
		
		keyAndValue := strings.Split(line, " ")
		key := keyAndValue[0]
		value := keyAndValue[1:]
		if vSz, err = strconv.Atoi(value[2]); err != nil {
			return nil, err
		}
		if vPos, _ = strconv.Atoi(value[2]); err != nil {
			return nil, err
		}
		if t, _ = time.Parse(time.RFC3339, value[3]); err != nil {
			return nil, err
		}
		
		keydir[key] = Record{
			fileId: value[1],
			valueSize: uint(vSz),
			valuePosition: uint(vPos),
			timeStamp: t,
		}
	}

	return keydir, err
}

func new(directoryPath string, config []Config) (*BitCask, error) {
	var opts Config
	var file *os.File
	var err error
	var keydir Keydir
	pendingWrites = make(map[string][]byte)

	if config == nil {
		opts = DefaultConfig
	} else {
		opts = config[0]
	}

	filename := fmt.Sprintf("%d.cask", time.Now().UnixMilli())
	if opts.writePermission {
		file, err = os.OpenFile(path.Join(directoryPath, filename), os.O_CREATE, 0600)
	} else {
		file, err = os.OpenFile(path.Join(directoryPath, filename), os.O_CREATE, 0400)
	}
	if err != nil {
		return nil, err
	}

	keydirData, err := os.ReadFile(path.Join(directoryPath, "keydir.cask"))
	if err != nil {
		keydir = Keydir{}
	} else {
		if keydir, err = parseKeydirData(string(keydirData)); err != nil {
			return nil, err
		}
	}

	bc := &BitCask{
		activeFile: file,
		cursor: 0,
		dirName: directoryPath,
		keydir: keydir,
		config: opts,
	}

	return bc, err
}