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

func new(directoryPath string, config Config, lock string) (*BitCask, error) {
	var file *os.File
	var keydir Keydir
	var keydirData []byte

	if config.writePermission {
		file = newFile(directoryPath)
	}
	

	keydirData, err := os.ReadFile(path.Join(directoryPath, keydirFileName))
	if err != nil {
		keydir = Keydir{}
	} else {
		keydir = parseKeydirData(string(keydirData))
	}

	bc := &BitCask{
		activeFile: file,
		lock: lock,
		cursor: 0,
		dirName: directoryPath,
		keydir: keydir,
		config: config,
		pendingWrites: make(map[string][]byte),
	}

	return bc, err
}

func (bc *BitCask) buildKeydirFile() {
	keyDirFile, _ := os.Create(path.Join(bc.dirName, keydirFileName))

	for key, record := range bc.keydir {
		valueSize := strconv.Itoa(record.valueSize)
		valuePos := strconv.Itoa(int(record.valuePosition))
		t := record.timeStamp.Format(time.RFC3339)

		line := key + " " + record.fileId + " " + valueSize + " " + valuePos + " " + t
		fmt.Fprintln(keyDirFile, line)
	}
}

func parseKeydirData(keydirData string) Keydir {
	var keydir Keydir = Keydir{}
	var vSz int
	var vPos int
	var t time.Time
	scanner := bufio.NewScanner(strings.NewReader(keydirData))

	for scanner.Scan() {
		line := scanner.Text()
		
		keyAndValue := strings.Split(line, " ")
		key := keyAndValue[0]
		fileId := keyAndValue[1]
		vSz, _ = strconv.Atoi(keyAndValue[2])
		vPos, _ = strconv.Atoi(keyAndValue[3])
		t, _ = time.Parse(time.RFC3339, keyAndValue[4])
		
		keydir[key] = Record{
			fileId: fileId,
			valueSize: vSz,
			valuePosition: int64(vPos),
			timeStamp: t,
		}
	}

	return keydir
}

func newFile(directoryPath string) (*os.File){
	var file *os.File
	filename := fmt.Sprintf("%d" + BitCaskFileExtension, time.Now().UnixMicro())

	file, _ = os.OpenFile(path.Join(directoryPath, filename), 
							os.O_CREATE | os.O_RDWR, 
							0600)

	return file
}

func (bc *BitCask) isExist(key []byte) error {
	if _, ok := bc.keydir[string(key)]; !ok {
		return BitCaskError(fmt.Sprintf("%q: %s", string(key), ErrKeyNotExist.Error()))
	}
	return nil
}

func (bc *BitCask) loadToPendingWrites(key, value []byte) {
	
	bc.pendingWrites[string(key)] = value
}

func (bc *BitCask) makeItem(key, value []byte, timeStamp time.Time) []byte {
	tStamp := uint32(timeStamp.UnixMicro())
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

func (bc *BitCask) updateKeydirRecord (key, value []byte, fileName string, currentCursorPos int64, tStamp time.Time) {
	bc.keydir[string(key)] = Record {
		fileId: fileName,
		valueSize: len(value),
		valuePosition: int64(currentCursorPos + 16 + int64(len(key))),
		timeStamp: tStamp,
	}
}

func (bc *BitCask) appendItemToFile(item []byte, currentCursorPos *int64, file **os.File) int64 {
	if int64(len(item)) + (*currentCursorPos) > MaxFileSize {
		(*file).Close()

		*file = newFile(bc.dirName)
		*currentCursorPos = 0
	}
	valuePosition := *currentCursorPos
	n, _ := (*file).Write(item)
	*currentCursorPos += int64(n)

	return valuePosition
}

func (bc *BitCask) deleteOldFiles(oldFiles map[string] void) {
	for oldFile := range oldFiles {
		os.Remove(path.Join(bc.dirName, oldFile))
	}
}

func checkLock(dirName string) processAccess {
	bitcaskDirectory, _ := os.Open(dirName)
	files, _ := bitcaskDirectory.Readdir(0)

	for _, fileInfo := range files {
		if strings.HasPrefix(fileInfo.Name(), readLock) {
			return reader
		} else if strings.HasPrefix(fileInfo.Name(), writeLock) {
			return writer
		}
	}

	return noProcess
}
