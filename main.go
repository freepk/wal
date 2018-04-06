package main

import (
        "fmt"
        "log"
        "net/http"
        "os"
        "os/signal"
        "runtime"
        "sync/atomic"
        "syscall"
        "time"
)

const (
        walQueueSize    = 0x1000
        bytesBufferSize = 0x8000
)

var (
        counter uint64 = 0
        writer  *wal
        bytes   []byte
)

type empty struct{}

type record struct {
        lsn  uint64
        done chan empty
}

func newRecord() *record {
        return &record{lsn: 0,
                done: make(chan empty)}
}

func (r *record) waitDone() {
        <-r.done
        close(r.done)
}

type wal struct {
        lsn    uint64
        queue  chan *record
        buffer []*record
        file   *os.File
}

func newWal(name string) *wal {
        var err error
        var file *os.File
        if file, err = os.Create(name); err != nil {
                log.Fatal("Cannot create file, error: %v", err)
        }
        return &wal{
                lsn:    0,
                queue:  make(chan *record, walQueueSize),
                buffer: make([]*record, walQueueSize),
                file:   file}
}

func (w *wal) enqueue() *record {
        var r *record
        r = newRecord()
        w.queue <- r
        return r
}

func (w *wal) dequeue() {
        var i, j int
        var lsn uint64
        // Copy operations-batches from queue to local buffer
        i = 0
        for {
                select {
                case w.buffer[i] = <-w.queue:
                        i++
                        continue
                default:
                }
                break
        }
        if i <= 0 {
                return
        }
        lsn = atomic.AddUint64(&w.lsn, 1)
        // Validate operations by CAS, sequentaly check all keys by version changing
        // if one of used key has greate version, operation-batch is invalid, skip them.
        // Merge all valid operations to log-record buffer parts like in leveldb
        // and write to log file.
        for j = 0; j < i/16; j++ {
                w.file.Write(bytes)
        }
        w.file.Sync()
        // Iterate all success ops and send commit notify.
        for j = 0; j < i; j++ {
                w.buffer[j].lsn = lsn
                w.buffer[j].done <- empty{}
        }
}

func (w *wal) Close() {
        w.file.Close()
}

func putHandler(w http.ResponseWriter, r *http.Request) {
        var rec *record
        rec = writer.enqueue()
        rec.waitDone()
        fmt.Fprintf(w, "%d", rec.lsn)
}

func termWaitInit() {
        var terminate chan os.Signal
        terminate = make(chan os.Signal)
        go func() {
                select {
                case <-terminate:
                        if writer != nil {
                                writer.Close()
                        }
                        os.Exit(0)
                }
        }()
        signal.Notify(terminate, syscall.SIGINT, syscall.SIGTERM)
}

func writerInit() {
        bytes = make([]byte, bytesBufferSize)
        writer = newWal("./tempWAL")
}


func main() {
        termWaitInit()
        writerInit()
        runtime.GOMAXPROCS(runtime.NumCPU())
        go func() {
                for {
                        writer.dequeue()
                        time.Sleep(time.Millisecond)
                }
        }()
        http.HandleFunc("/put", putHandler)
        http.ListenAndServe(":8080", nil)
}
