package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/gin-gonic/gin"
	"gopkg.in/yaml.v2"

	x "flowmaker/databuild"

	badger "github.com/dgraph-io/badger/v3"
)

// Config - data structure for the yaml config file information
type Config struct {
	WebPort int `yaml:"webport"`
}

type Params struct {
	Params string `uri:"params" binding:"required"`
}

type columnSend struct {
	Source  string
	Columns [][]string
	Tables  [][]string
}

func main() {
	// create flag to get run type, if added run in test mode
	boolTest := flag.Bool("test", false, "a bool to show if code needs to run in test mode add '-test'")
	flag.Parse()

	// load config yaml file
	filename, _ := filepath.Abs("config.yml")
	yamlFile, err := ioutil.ReadFile(filename)
	if err != nil {
		panic(err)
	}

	// marshal config file into variable
	var config Config
	err = yaml.Unmarshal(yamlFile, &config)
	if err != nil {
		panic(err)
	}

	dbKV, err := badger.Open(badger.DefaultOptions("tmp/badger"))
	if err != nil {
		panic(err)
	}
	dbKV.DropAll()
	dbKV.Close()

	// check to see if the '-test' flag has been added - if so print out results
	if !*boolTest {
		gin.DisableConsoleColor()
		gin.SetMode(gin.ReleaseMode)
		gin.DefaultWriter = ioutil.Discard
	}

	r := gin.Default()

	r.Static("/assets", "./assets")
	r.LoadHTMLGlob("templates/*")
	// r.StaticFile("/favicon.ico", "./favicon.ico")

	r.GET("/", func(c *gin.Context) {
		c.HTML(http.StatusOK, "index.tmpl", gin.H{
			"title": "FlowMaker",
		})
	})

	r.POST("/gettables", func(c *gin.Context) {

		var entry x.EntryDataSource
		if err := c.ShouldBindJSON(&entry); err != nil {
			c.JSON(400, gin.H{"msg": err.Error(), "response": 400})
			return
		}

		var datasend columnSend
		var cols []string

		if entry.Connection == "MS" {
			// cols, err = x.GetColumns()
		} else {
			cols, err = x.GetPSTables(entry)
		}
		if err != nil {
			c.JSON(400, gin.H{"msg": err.Error(), "response": 400})
			return
		}

		datasend.Tables = append(datasend.Tables, cols)

		datasend.Source = entry.Source

		c.JSON(200, gin.H{
			"data":     datasend,
			"response": 200,
		})

	})

	r.POST("/getcolumns", func(c *gin.Context) {

		var entry x.Entry
		if err := c.ShouldBindJSON(&entry); err != nil {
			c.JSON(400, gin.H{"msg": err.Error(), "response": 400})
			return
		}

		var datasend columnSend
		var cols []string

		if entry.Connection == "MS" {
			// cols, err = x.GetColumns()
		} else {
			cols, err = x.GetPSColumns(entry)
		}
		if err != nil {
			c.JSON(400, gin.H{"msg": err.Error(), "response": 400})
			return
		}

		datasend.Columns = append(datasend.Columns, cols)

		c.JSON(200, gin.H{
			"data":     datasend,
			"response": 200,
		})

	})

	r.POST("/datarun", func(c *gin.Context) {

		var entry []map[string]interface{}
		if err := c.ShouldBindJSON(&entry); err != nil {
			c.JSON(400, gin.H{"msg": err.Error(), "response": 400})
			return
		}

		dbKV, err := badger.Open(badger.DefaultOptions("tmp/badger"))
		if err != nil {
			dbKV.Close()
			c.JSON(400, gin.H{"msg": err.Error(), "response": 400})
			return
		}
		// defer dbKV.Close()
		dbKV.DropAll()
		dbKV.Close()

		// fmt.Println(entry)

		for i, v := range entry {
			fmt.Printf("index: %v -------------------------------\n", i)
			fmt.Printf("value: %v\n", v["data"])
			err := x.RunDataChange(v["data"])
			fmt.Println("returned from command with:")
			fmt.Println(err)
			if err != nil {
				c.JSON(400, gin.H{"msg": err.Error(), "response": 400})
				return
			}

			dbKV, err := badger.Open(badger.DefaultOptions("tmp/badger"))
			if err != nil {
				// dbKV.Close()
				c.JSON(400, gin.H{"msg": err.Error(), "response": 400})
				return
			}
			// defer dbKV.Close()

			err = dbKV.View(func(txn *badger.Txn) error {
				opts := badger.DefaultIteratorOptions
				opts.PrefetchSize = 10
				it := txn.NewIterator(opts)
				defer it.Close()
				for it.Rewind(); it.Valid(); it.Next() {
					item := it.Item()
					k := item.Key()

					fmt.Println("returned from command with:")

					err := item.Value(func(v []byte) error {

						fmt.Println("UPDATE AFTER COMMAND")
						fmt.Printf("key=%s, value=%s\n", k, v)
						return nil
					})
					if err != nil {
						return err
					}
				}
				fmt.Println("returned from command with:")
				return nil
			})
			if err != nil {
				// dbKV.Close()
				c.JSON(400, gin.H{"msg": err.Error(), "response": 400})
				return
			}

			dbKV.Close()

		}

		f, err := os.Create("outFileName.csv")
		if err != nil {
			c.JSON(400, gin.H{"msg": err.Error(), "response": 400})
			return
		}
		defer f.Close()

		dbKV, err = badger.Open(badger.DefaultOptions("tmp/badger"))
		if err != nil {
			c.JSON(400, gin.H{"msg": err.Error(), "response": 400})
			dbKV.Close()
			return
		}
		defer dbKV.Close()

		firstRow := true

		err = dbKV.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchSize = 10
			it := txn.NewIterator(opts)
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				k := item.Key()
				err := item.Value(func(v []byte) error {
					fmt.Printf("key=%s, value=%s\n", k, v)
					w := csv.NewWriter(f)
					defer w.Flush()

					var row []string

					rowbase := strings.Split(string(v), "|")

					if firstRow {

						var rowHeaders []string

						for _, v := range rowbase {
							fmt.Println("OUPUT CHECK---------------")
							fmt.Println(v)
							valueStrip := strings.Split(v, ";")
							rowHeaders = append(rowHeaders, valueStrip[0])
						}

						if err := w.Write(rowHeaders); err != nil {
							return err
						}

						firstRow = false
					}

					for _, v := range rowbase {
						valueStrip := strings.Split(v, ";")
						row = append(row, valueStrip[1])
					}

					if err := w.Write(row); err != nil {
						return err
					}
					// write to CSV
					return nil
				})
				if err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			c.JSON(400, gin.H{"msg": err.Error(), "response": 400})
			return
		}

		f.Close()

		c.JSON(200, gin.H{
			"data":     "Completed build",
			"response": 200,
		})

	})

	if *boolTest {
		r.Run(fmt.Sprintf("127.0.0.1:%v", config.WebPort))
	} else {
		r.Run(fmt.Sprintf(":%v", config.WebPort))
	}
}
