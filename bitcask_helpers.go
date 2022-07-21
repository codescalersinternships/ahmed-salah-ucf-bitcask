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

func parseKeydirData(keydirData string) Keydir {
	var keydir Keydir = Keydir{}
	scanner := bufio.NewScanner(strings.NewReader(keydirData))

	for scanner.Scan() {
		line := scanner.Text()
		
		keyAndValue := strings.Split(line, " ")
		key := keyAndValue[0]
		value := keyAndValue[1:]
		vSz, _ := strconv.Atoi(value[2])
		vpos, _ := strconv.Atoi(value[2])
		t, _ := time.Parse(time.RFC3339, value[3])
		
		keydir[Key(key)] = Record{
			fileId: value[1],
			valueSize: uint(vSz),
			valuePosition: uint(vpos),
			timeStamp: t,
		}
	}

	return keydir
}

func new(directoryPath string, config []Config) (*BitCask, error) {
	var opts Config
	var file *os.File
	var err error
	var keydir Keydir

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
		keydir = parseKeydirData(string(keydirData))
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