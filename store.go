package main

import (
	"fmt"
	"time"
)

type value struct {
	data   string
	Expiry *time.Time
	Type   string
}

var store = map[string]value{}

func SET(key string, data string,duration time.Duration) {
	var expiry *time.Time 

	if duration > 0{
		e := time.Now().Add(duration)
		expiry = &e
	}

	store[key] = value{
		data:   data,
		Expiry: expiry,
		Type:   "string",
	}
}

func GET(key string)(string,bool) {
	val, ok := store[key]
	if !ok {
		return"key not found",false
	}
	if val.Expiry != nil && time.Now().After(*val.Expiry) {
		delete(store, key)
		
		return "key expired ", false
	}
	
	return val.data,true
}

func DEL(key string) bool {
	_, ok := store[key]
	if !ok {
		fmt.Println("key not found")
		return false
	}
	delete(store, key)
	fmt.Println("key is deleted")
	return true
}

func EXISTS(key string) bool {
	val, ok := store[key]
	if !ok {
		fmt.Println("key not found")
		return false
	}

	if val.Expiry != nil && time.Now().After(*val.Expiry) {
		fmt.Println("key not found")
		delete(store, key)
		return false
	}

	fmt.Println("key exists")
	return true

}
