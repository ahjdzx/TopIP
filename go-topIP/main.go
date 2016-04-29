package main

import (
	"bufio"
	"bytes"
	"hash/crc32"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
)

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
}

func main() {
	//	split(1000)

	topAll(10)
}

type Pair struct {
	ip    string
	count int
}

type IPList []*Pair

func (list IPList) Len() int {
	return len(list)
}

func (list IPList) Less(i, j int) bool {
	return list[i].count > list[j].count
}

func (list IPList) Swap(i, j int) {
	list[i], list[j] = list[j], list[i]
}

func topAll(num int) {
	files, err := ioutil.ReadDir("shards")
	if err != nil {
		log.Fatal(err)
	}

	pairs := make(map[string]chan *Pair)
	for _, file := range files {
		log.Println(file.Name())
		pairCh := make(chan *Pair, num)
		pairs[file.Name()] = pairCh
		go topIPList("shards/"+file.Name(), num, pairCh)
	}

	allIPList := IPList{}
	for _, pairCh := range pairs {
		for pair := range pairCh {
			allIPList = append(allIPList, pair)
		}
	}

	log.Println("sorting all ....")
	sort.Sort(allIPList)
	for i, pair := range allIPList {
		log.Println("ip:", pair.ip, ", count:", pair.count)
		if i == num {
			break
		}
	}
}

func topIPList(name string, topNum int, ipCh chan *Pair) {
	log.Println("counting top ", topNum, "in", name)
	f, err := os.Open(name)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	ips := make(map[string]int)
	rd := bufio.NewReader(f)
	lineNum := 0
	for {
		line, err := rd.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Println(err)
			return
		}
		lineNum++

		//		if lineNum%1000 == 0 {
		//			log.Println("lineNum:", lineNum)
		//		}

		line = strings.TrimSpace(line)
		if count, ok := ips[line]; !ok {
			ips[line] = 1
		} else {
			ips[line] = count + 1
		}
	}

	ipList := IPList{}
	for ip, count := range ips {
		ipList = append(ipList, &Pair{ip, count})
	}

	log.Println("sorting ... ")
	sort.Sort(ipList)

	for i := 0; i < topNum; i++ {
		ipCh <- ipList[i]
	}
	close(ipCh)
}

type File struct {
	f      *os.File
	lineCh chan string
}

func NewFile(f *os.File) *File {
	return &File{
		f:      f,
		lineCh: make(chan string),
	}
}

func (f *File) loop() {
	log.Println(f.f.Name() + "looping...")
	wt := bufio.NewWriter(f.f)
	count := 0
	for line := range f.lineCh {
		count++
		_, err := wt.WriteString(line)
		if err != nil {
			log.Println("error:", err)
		}
		if count%1000 == 0 {
			wt.Flush()
		}
	}
	wt.Flush()
}

func split(num uint32) {
	f, err := os.Open("ipAddr.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	rd := bufio.NewReader(f)
	count := 0
	files := make(map[uint32]*File)
	for {
		line, err := rd.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Println(err)
			return
		}
		count++

		if count%1e5 == 0 {
			log.Println("reading line:", count, line)
		}

		h := crc32.NewIEEE()
		h.Write([]byte(line))
		v := h.Sum32()
		mod := v % num

		file, ok := files[mod]
		if !ok {
			log.Println("mod:", mod, "not exists.")
			filepath := "shards/" + strconv.Itoa(int(mod)) + ".txt"
			f, err := os.Create(filepath)
			if err != nil {
				log.Printf("create file: %s failed: %s\n", filepath, err)
				return
			}
			defer f.Close()
			file = NewFile(f)
			go file.loop()
			files[mod] = file
		}
		file.lineCh <- line
	}

	for mod, file := range files {
		log.Println("close file chan: ", mod)
		close(file.lineCh)
	}
}

func createBigFile() {
	f, err := os.Create("ipAddr.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	var buf bytes.Buffer

	for i := 0; i < 1e8; i++ {
		buf.WriteString("10")
		for j := 0; j < 3; j++ {
			buf.WriteByte('.')
			a := rand.Intn(256)
			buf.WriteString(strconv.Itoa(a))
		}
		buf.WriteByte('\n')

		if i%10 == 0 {
			_, err := f.Write(buf.Bytes())
			if err != nil {
				log.Println(err)
				return
			}
			buf.Reset()
		}
	}
	_, err = f.Write(buf.Bytes())
	if err != nil {
		log.Println(err)
	}
}
