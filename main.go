package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/jcelliott/lumber"
)

const Version = "1.0.0"

type (
	Logger interface {
		Fatal(string, ...interface{})
		Error(string, ...interface{})
		Warn(string, ...interface{})
		Debug(string, ...interface{})
		Trace(string, ...interface{})
		Info(string, ...interface{})
	}
	Driver struct {
		mutex   sync.Mutex
		mutexes map[string]*sync.Mutex
		dir     string
		log     Logger
	}
)

type Options struct {
	Logger
}

func New(dir string, options *Options) (*Driver, error) {
	dir = filepath.Clean(dir)
	opts := Options{}
	if options != nil {
		opts = *options
	}
	if opts.Logger == nil {
		opts.Logger = lumber.NewConsoleLogger((lumber.INFO))
	}

	driver := Driver{
		dir:     dir,
		mutexes: make(map[string]*sync.Mutex),
		log:     opts.Logger,
	}
	if _, err := os.Stat(dir); err == nil {
		opts.Logger.Debug("Using '%s' (database already exixts)\n", dir)
		return &driver, nil
	}
	opts.Logger.Debug("Creating the databse at '%s'...\n", dir)
	return &driver, os.MkdirAll(dir, 0755)
}

func stat(path string) (fi os.FileInfo, err error) {
	if fi, err = os.Stat(path); os.IsNotExist(err) {
		fi, err = os.Stat(path + ".json")
	}
	return
}

func (d *Driver) Write(collection string, resource string, value interface{}) error {
	if collection == "" {
		return fmt.Errorf("Missing collection - no place to save records")
	}
	if resource == "" {
		return fmt.Errorf("Missing resource - unable to save records (no name)")
	}
	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()
	dir := filepath.Join(d.dir, collection)
	finalPath := filepath.Join(dir, resource+".json")
	tempPath := finalPath + ".tmp"

	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(value, "", "\t")
	if err != nil {
		return err
	}
	b = append(b, byte('\n'))

	if err := ioutil.WriteFile(tempPath, b, 0644); err != nil {
		return err
	}

	return os.Rename(tempPath, finalPath)
}

func (d *Driver) Read(collection string, resource string, value interface{}) error {
	if collection == "" {
		return fmt.Errorf("Missing collection - unable to read")
	}
	if resource == "" {
		return fmt.Errorf("Missing resource - unable to save record (no name)")
	}

	record := filepath.Join(d.dir, collection, resource)

	if _, err := stat(record); err != nil {
		return err
	}
	b, err := ioutil.ReadFile(record + ".json")
	if err != nil {
		return err
	}
	return json.Unmarshal(b, &value)
}

func (d *Driver) ReadAll(collection string) ([]string, error) {
	if collection == "" {
		return nil, fmt.Errorf("Missing collection - unable to read")
	}
	dir := filepath.Join(d.dir, collection)

	if _, err := stat(dir); err != nil {
		return nil, err
	}
	file, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var records []string
	for _, x := range file {
		data, err := ioutil.ReadFile(filepath.Join(dir, x.Name()))
		if err != nil {
			return nil, err
		}
		records = append(records, string(data))
	}
	return records, nil
}

func (d *Driver) Delete(collection string, resource string) error {
	path := filepath.Join(collection, resource)
	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(d.dir, path)

	switch fi, err := stat(dir); {
	case fi == nil, err != nil:
		return fmt.Errorf("unable to find file or directory named %v\n", path)
	case fi.Mode().IsDir():
		return os.RemoveAll(dir)
	case fi.Mode().IsRegular():
		return os.RemoveAll(dir + ".json")
	}
	return nil
}

func (d *Driver) getOrCreateMutex(collection string) *sync.Mutex {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	m, ok := d.mutexes[collection]
	if !ok {
		m = &sync.Mutex{}
		d.mutexes[collection] = m
	}
	return m
}

type Address struct {
	City    string
	State   string
	Country string
	Pincode json.Number
}

type User struct {
	Name    string
	Age     json.Number
	Contact string
	Company string
	Address Address
}

func main() {

	dir := "./"

	db, err := New(dir, nil)
	if err != nil {
		fmt.Println("Error ", err)
	}

	employees := []User{
		{"Ravi", "25", "2929292", "My tech", Address{"banglore", "karnatak", "india", "433221"}},
		{"Rahul", "24", "2929292", "Gojek", Address{"delhi", "karnatak", "india", "433221"}},
		{"Shivam", "23", "2929292", "Apple", Address{"chennai", "karnatak", "india", "433221"}},
		{"Kartik", "21", "2929292", "URI", Address{"indore", "karnatak", "india", "433221"}},
		{"Rohan", "34", "2929292", "FinTech", Address{"banglore", "karnatak", "india", "433221"}},
		{"Nitin", "21", "2929292", "War", Address{"pune", "karnatak", "india", "433221"}},
	}

	// use of write one function
	for _, value := range employees {
		db.Write("users", value.Name, User{
			Name:    value.Name,
			Age:     value.Age,
			Contact: value.Contact,
			Company: value.Company,
			Address: value.Address,
		})
	}

	records, err := db.ReadAll("users")
	if err != nil {
		fmt.Println("Error ", err)
	}

	fmt.Println(records)

	allusers := []User{}

	for _, x := range records {
		employeesFound := User{}
		if err := json.Unmarshal([]byte(x), &employeesFound); err != nil {
			fmt.Println("Error ", err)
		}
		allusers = append(allusers, employeesFound)
	}
	fmt.Println(allusers)

	if err := db.Delete("users", "Rohan"); err != nil {
		fmt.Println("Error ", err)
	}

	// if err := db.Delete("users", ""); err != nil {
	// 	fmt.Println("Error ", err)
	// }

}
