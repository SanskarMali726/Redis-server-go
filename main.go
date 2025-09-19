package main

import (
	//"bufio"
	"fmt"
	//"io"
	"net"
	//"os"
	"strings"
	//	"strconv"
	
)

func main() {

	l, err := net.Listen("tcp", ":6379")

	fmt.Println("Listening on port 6379")

	if err != nil {
		fmt.Println(err)
		return
	}

	aof,err := NewAof("database.aof")
	if err != nil{
		fmt.Println(err)
		return 
	}
	defer aof.Close()

	conn, err := l.Accept()
	if err != nil {
		fmt.Println(err)
		return
	}

	defer conn.Close()

	for {
		resp := NewResp(conn)
		value,err := resp.Read()
		if err != nil {
			fmt.Println(err)
			break
		}
		
		if value.typ != "array"{
			fmt.Println("Invalid request, Expected array")
			continue
		}

		if len(value.array) == 0{
			fmt.Println("Invalid request,Expected array length > 0")
			continue
		}


		aof.Read(func(value Value) {
			command := strings.ToUpper(value.array[0].bulk)
			args := value.array[1:]

			handler, ok := Handlers[command]
			if !ok {
				fmt.Println("Invalid command: ", command)
				return
			}
			handler(args)
		})
	
		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]

		writer := NewWriter(conn) //created new writer to write the things in connection

		handler,ok := Handlers[command]
		if !ok {
			fmt.Println("Invalid command:",command)
			writer.Write(Value{typ: "string",str: ""})
			continue
		}

		if command == "SET" || command == "HSET" || command == "DEL"{
			
			aof.Write(value)
		}

		result := handler(args)
		writer.Write(result)
				
	}

	//input := "$5\r\nAhmed\r\n"

	//reader := bufio.NewReader(strings.NewReader(input))

	//b,_:= reader.ReadByte()
	//if b != '$' {
	//	fmt.Println("INVALID DATA TYPE,expecting bulk strings only")
	//	os.Exit(1)
	//}

	//size,_ := reader.ReadByte()

	//strSize,_ := strconv.ParseInt(string(size),10,64)

	//reader.ReadByte()
	//reader.ReadByte()

	//name := make([]byte,strSize)

	//reader.Read(name)

	//fmt.Println(string(name))
}
