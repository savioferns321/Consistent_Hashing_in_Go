package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/cznic/sortutil"
	"github.com/drone/routes"

	"github.com/spaolacci/murmur3"
)

type MapValueResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

var nodeMap map[uint64]string
var keys sortutil.Uint64Slice

func insertValue(w http.ResponseWriter, r *http.Request) {

	fmt.Println("Entered insertvalue")
	key := r.URL.Query().Get(":keyID")
	value := r.URL.Query().Get(":value")
	address := getNode(key)
	var buffer bytes.Buffer
	buffer.WriteString(address)
	buffer.WriteString("keys")
	buffer.WriteString("/")
	buffer.WriteString(key)
	buffer.WriteString("/")
	buffer.WriteString(value)

	//Create a request
	req, err := http.NewRequest("PUT", buffer.String(), nil)
	if err != nil {
		fmt.Println("error: body, _ := ioutil.ReadAll(resp.Body) -- line 592")
		panic(err)
	}
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("error: Unable to submit request")
		panic(err)
	}
	defer resp.Body.Close()
	w.WriteHeader(resp.StatusCode)
	if resp.StatusCode != http.StatusOK {
		fmt.Println("Data not persisted")
		panic(err.Error())
	} else {
		fmt.Println("Key : ", key, " || Value : ", value, " --- added to node : ", address)
	}

}

func getValue(w http.ResponseWriter, r *http.Request) {

	key := r.URL.Query().Get(":keyID")
	address := getNode(key)
	var buffer bytes.Buffer
	buffer.WriteString(address)
	buffer.WriteString("keys")
	buffer.WriteString("/")
	buffer.WriteString(key)

	//Create a request
	req, err := http.NewRequest("GET", buffer.String(), nil)
	if err != nil {
		fmt.Println("error: body, _ := ioutil.ReadAll(resp.Body) -- line 592")
		panic(err)
	}
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("error: Unable to submit request")
		panic(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		var outputResponse MapValueResponse

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			fmt.Println("error while reading body")
			panic(err.Error())
		}

		err = json.Unmarshal(body, &outputResponse)
		if err != nil {
			fmt.Println("error: Unable to unmarshal JSON")
			panic(err.Error())
		}
		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Type", "application/json")
		outputJSON, err := json.Marshal(outputResponse)
		if err != nil {
			w.Write([]byte(`{    "error": "Unable to marshal response.`))
			panic(err.Error())
		}
		fmt.Println("Retrieved from node : ", address, " for key : ", key)
		w.Write(outputJSON)
	} else {
		w.WriteHeader(resp.StatusCode)
	}

}

func getNode(key string) string {
	keyHash := murmur3.Sum64([]byte(key))
	var returnIndex = len(keys) - 1
	for index, element := range keys {
		if keyHash < element {
			if index > 0 {
				returnIndex = index - 1
			}
			break

		}
	}
	return nodeMap[keys[returnIndex]]

}

func main() {
	nodeMap = make(map[uint64]string)
	node1 := "http://localhost:3000/"
	node2 := "http://localhost:3001/"
	node3 := "http://localhost:3002/"

	//Sort the map

	keys = append(keys, murmur3.Sum64([]byte(node1)))
	keys = append(keys, murmur3.Sum64([]byte(node2)))
	keys = append(keys, murmur3.Sum64([]byte(node3)))

	keys.Sort()
	for index, element := range keys {
		fmt.Println(index, " --> ", element)
	}

	for _, element := range keys {
		switch element {

		case murmur3.Sum64([]byte(node1)):
			nodeMap[element] = node1
		case murmur3.Sum64([]byte(node2)):
			nodeMap[element] = node2
		case murmur3.Sum64([]byte(node3)):
			nodeMap[element] = node3
		}

	}

	mux := routes.New()
	mux.Put("/keys/:keyID/:value", insertValue)
	mux.Get("/keys/:keyID", getValue)

	http.Handle("/", mux)
	http.ListenAndServe(":8088", nil)

}
