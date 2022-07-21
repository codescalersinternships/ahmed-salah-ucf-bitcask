package bitcask

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)


func (bc *BitCask) isExist(key []byte) error {
	if _, ok := bc.keydir[string(key)]; !ok {
		return BitCaskError(fmt.Sprintf("%q: %s", string(key), ErrKeyNotExist.Error()))
	}
	return nil
}

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
		if vSz, err = strconv.Atoi(value[1]); err != nil {
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
			valueSize: vSz,
			valuePosition: int64(vPos),
			timeStamp: t,
		}
	}

	return keydir, err
}

func newActiveFile(directoryPath string, config Config) (*os.File, error){
	var file *os.File
	var err error
	filename := fmt.Sprintf("%d.cask", time.Now().UnixMilli())
	if config.writePermission {
		file, err = os.OpenFile(path.Join(directoryPath, filename), os.O_CREATE, 0600)
	} else {
		file, err = os.OpenFile(path.Join(directoryPath, filename), os.O_CREATE, 0400)
	}
	if err != nil {
		return nil, err
	}

	return file, err
}

func new(directoryPath string, config []Config) (*BitCask, error) {
	var opts Config
	var file *os.File
	var err error
	var keydir Keydir
	var keydirData []byte
	pendingWrites = make(map[string][]byte)

	if config == nil {
		opts = DefaultConfig
	} else {
		opts = config[0]
	}

	file, err = newActiveFile(directoryPath, opts)
	if err != nil {
		return nil, err
	}

	keydirData, err = os.ReadFile(path.Join(directoryPath, "keydir.cask"))
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

func (bc *BitCask) makeItem(key, value []byte, timeStamp time.Time) []byte {
	tStamp := uint32(timeStamp.Unix())
	keySize := uint32(len(key))
	valueSize := uint32(len(value))

	item := make([]byte, 16, 16+keySize+valueSize)

	binary.BigEndian.PutUint32(item[4:], tStamp)
	binary.BigEndian.PutUint32(item[8:], keySize)
	binary.BigEndian.PutUint32(item[12:], valueSize)

	item = append(item, key...)
	item = append(item, value...)

	crc := crc32.ChecksumIEEE(item[4:])
	binary.BigEndian.PutUint32(item[:], crc)

	return item
}

func (bc *BitCask) loadToPendingWrites(key, value []byte) error {
	var err error
	if len(pendingWrites) > MaxPendingSize {
		err = bc.Sync()
		for k := range pendingWrites {
			delete(pendingWrites, k)
		}
		pendingWrites[string(key)] = value
	}
	return err
}

func (bc *BitCask) appendItemToActiveFile(item []byte) error {
	var err error
	var activeFile *os.File
	var n int
	if bc.cursor+len(item) > MaxFileSize {
		activeFile, err = newActiveFile(bc.dirName, bc.config)
		if err != nil {
			return err
		}
		bc.activeFile = activeFile
		bc.cursor = 0
	}

	n, err = bc.activeFile.Write(item)
	if err != nil {
		return err
	}
	bc.cursor += n

	return nil
}

func (bc *BitCask) updateKeydir (key, value []byte, tStamp time.Time) {
	bc.keydir[string(key)] = Record {
		fileId: bc.activeFile.Name(),
		valueSize: len(value),
		valuePosition: int64(bc.cursor + 16 + len(key)),
		timeStamp: tStamp,
	}
}

func (bc *BitCask) appendItem(key, value []byte) error {

	tStamp := time.Now()
	item := bc.makeItem(key, value, tStamp)
	
	err := bc.appendItemToActiveFile(item)
	if err != nil {
		return err
	}

	bc.updateKeydir(key, value, tStamp)

	return nil
}