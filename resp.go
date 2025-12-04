package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"sync"
)

const (
	STRING  = '+'
	ERROR   = '-'
	INTEGER = ':'
	BULK    = '$'
	ARRAY   = '*'
)

type Value struct {
	typ   string
	str   string
	num   int
	bulk  string
	array []Value
}

type Resp struct {
	reader *bufio.Reader
}

func NewResp(rd io.Reader) *Resp {
	return &Resp{reader: bufio.NewReader(rd)}
}

func (r *Resp) readLine() (line []byte, n int, err error) {
	for {
		b, err := r.reader.ReadByte()

		if err != nil {
			return nil, 0, err
		}
		n = +1
		line = append(line, b)
		if len(line) >= 2 && line[len(line)-2] == '\r' {
			break
		}

	}
	return line[:len(line)-2], n, nil
}

func (r *Resp) readInteger() (x int, n int, err error) {
	line, n, err := r.readLine()

	if err != nil {
		return 0, 0, err
	}

	i64, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return 0, n, err
	}

	return int(i64), n, nil
}

func (r *Resp) Read() (Value, error) {
	_type, err := r.reader.ReadByte()
	if err != nil {
		return Value{}, err
	}

	switch _type {
	case ARRAY:
		return r.readArray()
	case BULK:
		return r.readBulk()
	default:
		fmt.Printf("Unknown type %v", string(_type))
		return Value{}, nil
	}
}

func (r *Resp) readArray() (Value, error) {
	v := Value{}
	v.typ = "array"
	length, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	v.array = make([]Value, length)
	for i := 0; i < length; i++ {
		val, err := r.Read()
		if err != nil {
			return v, err
		}

		v.array[i] = val
	}

	return v, nil
}

func (r *Resp) readBulk() (Value, error) {
	v := Value{}
	v.typ = "bulk"

	length, _, err := r.readInteger()
	if err != nil {
		return v, err
	}
	bulk := make([]byte, length)

	r.reader.Read(bulk)

	v.bulk = string(bulk)

	r.readLine()

	return v, err
}

func (v Value) Marshal() []byte {
	switch v.typ {
	case "array":
		return v.marshalArray()
	case "bulk":
		return v.marshalBulk()
	case "string":
		return v.marshalString()
	case "null":
		return v.marshalNull()
	case "error":
		return v.marshalError()
	default:
		return []byte{}
	}
}

func (v Value) marshalString() []byte {
	var bytes []byte
	bytes = append(bytes, STRING)
	bytes = append(bytes, v.str...) // we use this ... to append string into a single byte form like if string is sanskar the s will append sepearately and then other latters will be appended separately
	bytes = append(bytes, '\r', '\n')
	return bytes
}

func (v Value) marshalBulk() []byte {
	var bytes []byte
	bytes = append(bytes, BULK)
	bytes = append(bytes, strconv.Itoa(len(v.bulk))...)
	bytes = append(bytes, '\r', '\n')
	bytes = append(bytes, v.bulk...)
	bytes = append(bytes, '\r', '\n')
	return bytes
}

func (v Value) marshalArray() []byte {
	len := len(v.array)
	var bytes []byte

	bytes = append(bytes, ARRAY)
	bytes = append(bytes, strconv.Itoa(len)...)
	bytes = append(bytes, '\r', '\n')

	for i := 0; i < len; i++ {
		bytes = append(bytes, v.array[i].Marshal()...)
	}
	return bytes
}

func (v Value) marshalNull() []byte {
	return []byte("$-1\r\n")
}

func (v Value) marshalError() []byte {
	var bytes []byte
	bytes = append(bytes, ERROR)
	bytes = append(bytes, v.str...)
	bytes = append(bytes, '\r', '\n')
	return bytes
}

type Writer struct{
	writer io.Writer 
}

func NewWriter(w io.Writer) *Writer{
	return &Writer{writer: w}
}

func (w *Writer) Write(v Value) error {
	var bytes = v.Marshal()
	_,err := w.writer.Write(bytes)
	if err != nil{
		return err
	}
	return nil
}

var Handlers = map[string]func([]Value) Value{
	"PING":ping ,
	"SET":set,
	"GET":get,
	"DEL":del,
	"EXISTS":exists,
}

func ping(args []Value) Value {
	if len(args) == 0{
		return Value{typ: "string",str: "PONG"}
	}
	return Value{typ: "string",str: args[0].bulk}
} 

var SETs = map[string]string{}
var SETsMu = sync.RWMutex{}

func set(args []Value) Value{
	if len(args) != 2{
		return Value{typ: "error",str: "ERR wrong number of arguments for 'set' command"}
	}

	key := args[0].bulk
	value := args[1].bulk

	SETsMu.Lock()
	SETs[key] = value
	SETsMu.Unlock()

	return Value{typ: "string",str: "OK"}
}

func get(args []Value) Value{
	if len(args) != 1{
		return Value{typ: "error", str: "ERR wrong number of arguments for 'get' command"}
	}

	key := args[0].bulk

	SETsMu.RLock()
	value,ok:= SETs[key]
	SETsMu.RUnlock()

	if !ok{
		return Value{typ: "null"}
	}

	return Value{typ: "bulk",bulk: value}
}

func del(args []Value) Value{
	if len(args) != 1{
		return Value{typ:"error", str: "ERR wrong number of arguments for 'del' command" }
	}

	key := args[0].bulk	
	
	_,ok := SETs[key]
	if !ok {
		return Value{typ: "string",str: "OK"}
	}

	SETsMu.Lock()
		delete(SETs,key)
	SETsMu.Unlock()

	return Value{typ:"string",str: "KEY deleted successfuly"}
}

func exists(args []Value)Value{
	if len(args) != 1{
		return Value{typ:"error", str: "ERR wrong number of arguments for 'exists' command" }
	}

	key := args[0].bulk

	SETsMu.RLock()
	_,ok := SETs[key]
	if !ok {
		return Value{typ: "error",str: "KEY not found"}
	}
	SETsMu.RUnlock()

	return Value{typ:"string",str: "KEY Exists"}
}