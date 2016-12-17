package main

import (
	//"encoding/json"
	//"fmt"
	//"log"
	"io/ioutil"
	"net/http"
	//"strconv"
)

type Foo struct {
	Bar string
}

func getJson(url string) ([]byte, error) {
	var s []byte
	r, err := http.Get(url)
	if err != nil {
		return s, err
	}
	defer r.Body.Close()

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
	    return s, err
	}

	return body, err
}

func main() {
	url := "https://hacker-news.firebaseio.com/v0/item/12980380.json"
	body, _ := getJson(url)
	s := string(body[:])
	println(s)
}
