package databuild

import (
	"database/sql"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"

	badger "github.com/dgraph-io/badger/v3"
)

type Entry struct {
	Source     string `form:"source" json:"source" xml:"source"  binding:"required"`
	Connection string `form:"dataconnection" json:"dataconnection" xml:"dataconnection"  binding:"required"`
	Table      string `form:"table" json:"table" xml:"table"  binding:"required"`
	DBHost     string `form:"dbhost" json:"dbhost" xml:"dbhost"`
	DBPort     string `form:"dbport" json:"dbport" xml:"dbport"`
	DBUser     string `form:"dbuser" json:"dbuser" xml:"dbuser"`
	DBPassword string `form:"dbpassword" json:"dbpassword" xml:"dbpassword"`
	DBDatabase string `form:"dbdatabase" json:"dbdatabase" xml:"dbdatabase"`
	DBSQL      string `form:"dbsql" json:"dbsql" xml:"dbsql"`
}

type EntryDataSource struct {
	Source     string `form:"source" json:"source" xml:"source" binding:"required"`
	Connection string `form:"dataconnection" json:"dataconnection" xml:"dataconnection" binding:"required"`
	DBHost     string `form:"dbhost" json:"dbhost" xml:"dbhost" binding:"required"`
	DBPort     string `form:"dbport" json:"dbport" xml:"dbport" binding:"required"`
	DBUser     string `form:"dbuser" json:"dbuser" xml:"dbuser" binding:"required"`
	DBPassword string `form:"dbpassword" json:"dbpassword" xml:"dbpassword" binding:"required"`
	DBDatabase string `form:"dbdatabase" json:"dbdatabase" xml:"dbdatabase" binding:"required"`
}

var counter int

func RunDataChange(command interface{}) (err error) {

	option := reflect.TypeOf(command).String()

	if option == "map[string]interface {}" {
		m := command.(map[string]interface{})

		switch m["code"] {
		case "#00#":

			if m["dataconnection"].(string) == "MS" {
				// dfs[m["source"].(string)] = df
			} else {
				err = GetPSData(m)
			}

			if err != nil {
				return err
			}

		case "#01#":

			// TODO: join on a column

			tempTable := make(map[string][]byte)

			dbKV, err := badger.Open(badger.DefaultOptions("tmp/badger"))
			if err != nil {
				dbKV.Close()
				return err
			}
			defer dbKV.Close()

			// loop and ge the 2nd table
			err = dbKV.Update(func(txn *badger.Txn) error {
				opts := badger.DefaultIteratorOptions
				opts.PrefetchSize = 10
				it := txn.NewIterator(opts)
				defer it.Close()
				for it.Rewind(); it.Valid(); it.Next() {
					item := it.Item()
					k := item.Key()
					err := item.Value(func(v []byte) error {

						if strings.Contains(string(v), m["sourceb"].(string)) {

							tempTable[string(k)] = v

							err := txn.Delete([]byte(string(k)))
							if err != nil {
								return err
							}
						}

						return nil
					})
					if err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				return err
			}

			// 2nd loop to then loop the tempTable

			err = dbKV.Update(func(txn *badger.Txn) error {
				opts := badger.DefaultIteratorOptions
				opts.PrefetchSize = 10
				it := txn.NewIterator(opts)
				defer it.Close()
				for it.Rewind(); it.Valid(); it.Next() {
					item := it.Item()
					k := item.Key()
					err := item.Value(func(v []byte) error {

						if strings.Contains(string(v), m["sourcea"].(string)) {

							matchedCouner := 0

							getColumns := strings.Split(string(v), "|")

							for _, v2 := range getColumns {
								colData := strings.Split(v2, ";")

								if colData[1] == m["columna"].(string) {

									for _, v3 := range tempTable {
										if strings.Contains(string(v3), fmt.Sprintf("%v;%v", colData[1], colData[2])) {
											matchedCouner++

											e := badger.NewEntry([]byte(fmt.Sprintf("%v.%v", string(k), matchedCouner)), []byte(fmt.Sprintf("%v|%v", string(v), string(v3)))).WithTTL(time.Hour)
											err = txn.SetEntry(e)
											if err != nil {
												return err
											}
										}
									}

									// type of join determines what gets deleted
									switch m["join"].(string) {
									case "Inner":
										// delete row a if no match
										if matchedCouner == 0 {
											err := txn.Delete([]byte(string(k)))
											if err != nil {
												return err
											}
										}

									case "Left":
										// delete old row on match to leave new row
										if matchedCouner > 0 {
											err := txn.Delete([]byte(string(k)))
											if err != nil {
												return err
											}
										}
									}

								}

							}
						}

						return nil
					})
					if err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				return err
			}

		case "#04#":

			// fmt.Println(m)

			dbKV, err := badger.Open(badger.DefaultOptions("tmp/badger"))
			if err != nil {
				dbKV.Close()
				return err
			}
			defer dbKV.Close()

			err = dbKV.Update(func(txn *badger.Txn) error {
				opts := badger.DefaultIteratorOptions
				opts.PrefetchSize = 10
				it := txn.NewIterator(opts)
				defer it.Close()
				for it.Rewind(); it.Valid(); it.Next() {
					item := it.Item()
					k := item.Key()
					err := item.Value(func(v []byte) error {

						keeper := false

						for i, v2 := range m["filters"].([]interface{}) {
							fmt.Println(i)
							fmt.Println(v2)
							m2 := v2.(map[string]interface{})

							if strings.Contains(string(v), m["source"].(string)) {
								//matched db row
								getColumns := strings.Split(string(v), "|")

								for _, v3 := range getColumns {
									getColumnName := strings.Split(v3, ";")

									if m2["column"].(string) == getColumnName[1] {

										switch m2["operator"].(string) {
										case "Eq":
											if getColumnName[2] == m2["value"].(string) {
												keeper = true
											}
										case "Neq":
											if getColumnName[2] != m2["value"].(string) {
												keeper = true
											}
										case "Greater":
											if getColumnName[2] > m2["value"].(string) {
												keeper = true
											}
										case "GreaterEq":
											if getColumnName[2] >= m2["value"].(string) {
												keeper = true
											}
										case "Less":
											if getColumnName[2] < m2["value"].(string) {
												keeper = true
											}
										case "LessEq":
											if getColumnName[2] <= m2["value"].(string) {
												keeper = true
											}
										}

										if !keeper {
											err := txn.Delete([]byte(string(k)))
											if err != nil {
												return err
											}
										}

									}

								}
							}

						}

						// if strings.Contains(string(v), m["source"].(string)) {
						// 	//matched db row
						// 	getColumns := strings.Split(string(v), "|")

						// 	for _, v2 := range getColumns {
						// 		getColumnName := strings.Split(v2, ";")

						// 		if m["column"].(string) == getColumnName[1] {

						// 			keeper := false

						// 			switch m["operator"].(string) {
						// 			case "Eq":
						// 				if getColumnName[2] == m["value"].(string) {
						// 					keeper = true
						// 				}
						// 			case "Neq":
						// 				if getColumnName[2] != m["value"].(string) {
						// 					keeper = true
						// 				}
						// 			case "Greater":
						// 				if getColumnName[2] > m["value"].(string) {
						// 					keeper = true
						// 				}
						// 			case "GreaterEq":
						// 				if getColumnName[2] >= m["value"].(string) {
						// 					keeper = true
						// 				}
						// 			case "Less":
						// 				if getColumnName[2] < m["value"].(string) {
						// 					keeper = true
						// 				}
						// 			case "LessEq":
						// 				if getColumnName[2] <= m["value"].(string) {
						// 					keeper = true
						// 				}
						// 			case "In":
						// 				// com = series.In
						// 				// extra work in needed!
						// 				// TODO: here
						// 			}

						// 			if !keeper {

						// 				err := txn.Delete([]byte(string(k)))
						// 				if err != nil {
						// 					return err
						// 				}

						// 			}

						// 		}

						// 	}
						// }

						return nil
					})
					if err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				return err
			}

		case "#06#":

			cols := m["columns"].([]interface{})
			aggs := m["aggs"].([]interface{})
			colS := make(map[string]int)

			for _, v := range cols[0].([]interface{}) {
				colS[v.(string)] = 1
			}

			groupList := make(map[string]float64)

			dbKV, err := badger.Open(badger.DefaultOptions("tmp/badger"))
			if err != nil {
				dbKV.Close()
				return err
			}
			defer dbKV.Close()

			err = dbKV.Update(func(txn *badger.Txn) error {
				opts := badger.DefaultIteratorOptions
				opts.PrefetchSize = 10
				it := txn.NewIterator(opts)
				defer it.Close()
				for it.Rewind(); it.Valid(); it.Next() {
					item := it.Item()
					k := item.Key()
					var outputter []string
					var newIndex string

					err := item.Value(func(v []byte) error {

						if strings.Contains(string(v), m["source"].(string)) {
							//matched db row
							getColumns := strings.Split(string(v), "|")

							for _, v2 := range getColumns {
								getColumnName := strings.Split(v2, ";")

								_, keepCol := colS[getColumnName[1]]
								if keepCol {
									outputter = append(outputter, v2)
								}
							}

							newIndex = fmt.Sprintf("%v#%v", string(v), strings.Join(outputter, "|"))

						}

						return nil
					})
					if err != nil {
						return err
					}

					groupList[strings.Join(outputter, "|")] = 0

					e := badger.NewEntry(k, []byte(newIndex)).WithTTL(time.Hour)
					err = txn.SetEntry(e)
					if err != nil {
						return err
					}

				}
				return nil
			})
			if err != nil {
				return err
			}

			for _, v := range aggs {
				v2 := v.(map[string]interface{})

				err = dbKV.Update(func(txn *badger.Txn) error {
					opts := badger.DefaultIteratorOptions
					opts.PrefetchSize = 10
					it := txn.NewIterator(opts)
					defer it.Close()
					for it.Rewind(); it.Valid(); it.Next() {
						item := it.Item()
						k := item.Key()
						err := item.Value(func(v []byte) error {
							//matched db row

							for i2 := range groupList {

								if strings.Contains(string(v), i2) {
									// at correct row
									// need to get to agg column
									getColumns := strings.Split(string(v), "|")
									for _, v3 := range getColumns {
										getColumnName := strings.Split(v3, ";")
										if v2["column"].(string) == getColumnName[1] {
											// at right column

											val, _ := strconv.ParseFloat(getColumnName[2], 64)

											switch v2["agtype"].(string) {
											case "MAX":
												old := groupList[i2]
												if val > old {
													groupList[i2] = val
												}
											case "MEAN":
												// aggTypes = append(aggTypes, dataframe.Aggregation_MEAN)
											case "MEDIAN":
												// aggTypes = append(aggTypes, dataframe.Aggregation_MEDIAN)
											case "MIN":
												old := groupList[i2]
												if val < old {
													groupList[i2] = val
												}
											case "STD":
												// aggTypes = append(aggTypes, dataframe.Aggregation_STD)
											case "SUM":
												groupList[i2] += val
												// aggTypes = append(aggTypes, dataframe.Aggregation_SUM)
											case "COUNT":
												groupList[i2]++
												// aggTypes = append(aggTypes, dataframe.Aggregation_COUNT)
											}
										}
									}

								}
							}

							return nil
						})
						if err != nil {
							return err
						}

						err = txn.Delete([]byte(string(k)))
						if err != nil {
							return err
						}

					}
					return nil
				})
				if err != nil {
					return err
				}

			}

			counterA := 1

			for i, v := range groupList {
				err = dbKV.Update(func(txn *badger.Txn) error {
					e := badger.NewEntry([]byte(fmt.Sprintf("%02v", counterA)), []byte(fmt.Sprintf("%v;%v", i, v))).WithTTL(time.Hour)
					err := txn.SetEntry(e)
					return err
				})
				if err != nil {
					return err
				}

				counterA++
			}

		case "#08#":
			dbKV, err := badger.Open(badger.DefaultOptions("tmp/badger"))
			if err != nil {
				dbKV.Close()
				return err
			}
			defer dbKV.Close()

			err = dbKV.Update(func(txn *badger.Txn) error {
				opts := badger.DefaultIteratorOptions
				opts.PrefetchSize = 10
				it := txn.NewIterator(opts)
				defer it.Close()
				for it.Rewind(); it.Valid(); it.Next() {
					item := it.Item()
					k := item.Key()
					err := item.Value(func(v []byte) error {

						if strings.Contains(string(v), m["source"].(string)) {
							//matched db row
							getColumns := strings.Split(string(v), "|")

							for _, v2 := range getColumns {
								getColumnName := strings.Split(v2, ";")

								if m["column"].(string) == getColumnName[1] {

									keeper := false

									switch m["operator"].(string) {
									case "Eq":
										if getColumnName[2] == m["value"].(string) {
											keeper = true
										}
									case "Neq":
										if getColumnName[2] != m["value"].(string) {
											keeper = true
										}
									case "Greater":
										if getColumnName[2] > m["value"].(string) {
											keeper = true
										}
									case "GreaterEq":
										if getColumnName[2] >= m["value"].(string) {
											keeper = true
										}
									case "Less":
										if getColumnName[2] < m["value"].(string) {
											keeper = true
										}
									case "LessEq":
										if getColumnName[2] <= m["value"].(string) {
											keeper = true
										}
									case "In":
										// com = series.In
										// extra work in needed!
										// TODO: here
									}

									if !keeper {

										err := txn.Delete([]byte(string(k)))
										if err != nil {
											return err
										}

									}

								}

							}
						}

						return nil
					})
					if err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				return err
			}

		case "#10#":

			cols := m["columns"].([]interface{})
			colS := make(map[string]int)
			for _, v := range cols[0].([]interface{}) {
				colS[v.(string)] = 1
			}

			dbKV, err := badger.Open(badger.DefaultOptions("tmp/badger"))
			if err != nil {
				dbKV.Close()
				return err
			}
			defer dbKV.Close()

			err = dbKV.Update(func(txn *badger.Txn) error {
				opts := badger.DefaultIteratorOptions
				opts.PrefetchSize = 10
				it := txn.NewIterator(opts)
				defer it.Close()
				for it.Rewind(); it.Valid(); it.Next() {
					item := it.Item()
					k := item.Key()
					var outputter []string

					err := item.Value(func(v []byte) error {

						if strings.Contains(string(v), m["source"].(string)) {
							//matched db row
							getColumns := strings.Split(string(v), "|")

							for _, v2 := range getColumns {
								getColumnName := strings.Split(v2, ";")

								_, keepCol := colS[getColumnName[1]]
								if keepCol {
									outputter = append(outputter, v2)
								}

							}
						}

						return nil
					})
					if err != nil {
						return err
					}

					e := badger.NewEntry(k, []byte(strings.Join(outputter, "|"))).WithTTL(time.Hour)
					err = txn.SetEntry(e)
					if err != nil {
						return err
					}

				}
				return nil
			})
			if err != nil {
				return err
			}

		case "#11#":

			dbKV, err := badger.Open(badger.DefaultOptions("tmp/badger"))
			if err != nil {
				dbKV.Close()
				return err
			}
			defer dbKV.Close()

			tempTable := make(map[string][]byte)

			err = dbKV.View(func(txn *badger.Txn) error {
				opts := badger.DefaultIteratorOptions
				opts.PrefetchSize = 10
				it := txn.NewIterator(opts)
				defer it.Close()
				for it.Rewind(); it.Valid(); it.Next() {
					item := it.Item()
					err := item.Value(func(v []byte) error {

						if strings.Contains(string(v), m["source"].(string)) {

							getColumns := strings.Split(string(v), "|")
							var index string

							for _, v2 := range getColumns {
								getColumnName := strings.Split(v2, ";")

								if getColumnName[1] == m["column"].(string) {
									index = getColumnName[2]

								}

							}

							tempTable[index] = v

						}

						return nil
					})
					if err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				return err
			}

			keys := make([]string, 0, len(tempTable))
			for k := range tempTable {
				keys = append(keys, k)
			}

			switch m["direction"].(string) {
			case "ASC":
				sort.Strings(keys)
			case "DESC":
				sort.Sort(sort.Reverse(sort.StringSlice(keys)))
			}

			for i, k := range keys {
				err = dbKV.Update(func(txn *badger.Txn) error {
					e := badger.NewEntry([]byte(fmt.Sprintf("%02v", i+1)), tempTable[k]).WithTTL(time.Hour)
					err := txn.SetEntry(e)
					return err
				})
				if err != nil {
					return err
				}

			}

		case "#12#":

			dbKV, err := badger.Open(badger.DefaultOptions("tmp/badger"))
			if err != nil {
				dbKV.Close()
				return err
			}
			defer dbKV.Close()

			var maxRows int

			err = dbKV.View(func(txn *badger.Txn) error {
				opts := badger.DefaultIteratorOptions
				opts.PrefetchValues = false
				it := txn.NewIterator(opts)
				defer it.Close()
				for it.Rewind(); it.Valid(); it.Next() {
					maxRows++
				}
				return nil
			})
			if err != nil {
				return err
			}

			rows := make(map[string]string)

			numRows, err := strconv.Atoi(m["value"].(string))
			if err != nil {
				return err
				// break
			}

			if m["direction"].(string) == "First" {
				for i := 0; i < numRows; i++ {
					rows[fmt.Sprintf("%02v", i+1)] = fmt.Sprintf("%02v", i+1)
				}
			} else {
				// need to reverse counter
				lower := maxRows - numRows

				for i := lower; i < maxRows; i++ {
					rows[fmt.Sprintf("%02v", i+1)] = fmt.Sprintf("%02v", i+1)
				}
			}

			err = dbKV.Update(func(txn *badger.Txn) error {
				opts := badger.DefaultIteratorOptions
				opts.PrefetchValues = false
				it := txn.NewIterator(opts)
				defer it.Close()
				for it.Rewind(); it.Valid(); it.Next() {
					item := it.Item()
					k := item.Key()

					_, keepCol := rows[string(k)]
					if !keepCol {

						err := txn.Delete([]byte(string(k)))
						if err != nil {
							return err
						}

					}

				}
				return nil
			})
			if err != nil {
				return err
			}

		case "#13#":

			dbKV, err := badger.Open(badger.DefaultOptions("tmp/badger"))
			if err != nil {
				dbKV.Close()
				return err
			}
			defer dbKV.Close()

			err = dbKV.Update(func(txn *badger.Txn) error {
				opts := badger.DefaultIteratorOptions
				opts.PrefetchSize = 10
				it := txn.NewIterator(opts)
				defer it.Close()
				for it.Rewind(); it.Valid(); it.Next() {
					item := it.Item()
					k := item.Key()
					var outputter []string

					err := item.Value(func(v []byte) error {

						if strings.Contains(string(v), m["source"].(string)) {
							//matched db row
							getColumns := strings.Split(string(v), "|")

							for _, v2 := range getColumns {
								getColumnName := strings.Split(v2, ";")

								if getColumnName[1] == m["column"].(string) {
									outputter = append(outputter, fmt.Sprintf("%v;%v;%v", getColumnName[0], m["value"].(string), getColumnName[2]))
								} else {
									outputter = append(outputter, v2)
								}

							}
						}

						return nil
					})
					if err != nil {
						return err
					}

					e := badger.NewEntry(k, []byte(strings.Join(outputter, "|"))).WithTTL(time.Hour)
					err = txn.SetEntry(e)
					if err != nil {
						return err
					}

				}
				return nil
			})
			if err != nil {
				return err
			}

		}
	} else {
		m := command.([]interface{})
		code := m[0].(map[string]interface{})["code"].(string)

		switch code {
		case "#05#":

			dbKV, err := badger.Open(badger.DefaultOptions("tmp/badger"))
			if err != nil {
				dbKV.Close()
				return err
			}
			defer dbKV.Close()

			err = dbKV.Update(func(txn *badger.Txn) error {
				opts := badger.DefaultIteratorOptions
				opts.PrefetchSize = 10
				it := txn.NewIterator(opts)
				defer it.Close()
				for it.Rewind(); it.Valid(); it.Next() {
					item := it.Item()
					k := item.Key()
					err := item.Value(func(v []byte) error {

						keeper := false

						for _, v2 := range m {
							m2 := v2.(map[string]interface{})

							if strings.Contains(string(v), m2["source"].(string)) {
								//matched db row
								getColumns := strings.Split(string(v), "|")

								for _, v2 := range getColumns {
									getColumnName := strings.Split(v2, ";")

									if m2["column"].(string) == getColumnName[1] {

										switch m2["operator"].(string) {
										case "Eq":
											if getColumnName[2] == m2["value"].(string) {
												keeper = true
											}
										case "Neq":
											if getColumnName[2] != m2["value"].(string) {
												keeper = true
											}
										case "Greater":
											if getColumnName[2] > m2["value"].(string) {
												keeper = true
											}
										case "GreaterEq":
											if getColumnName[2] >= m2["value"].(string) {
												keeper = true
											}
										case "Less":
											if getColumnName[2] < m2["value"].(string) {
												keeper = true
											}
										case "LessEq":
											if getColumnName[2] <= m2["value"].(string) {
												keeper = true
											}
										case "In":
											// com = series.In
											// extra work in needed!
											// TODO: here
										}

									}

								}
							}

						}

						if !keeper {

							err := txn.Delete([]byte(string(k)))
							if err != nil {
								return err
							}

						}

						return nil
					})
					if err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				return err
			}

		case "#09#":

			dbKV, err := badger.Open(badger.DefaultOptions("tmp/badger"))
			if err != nil {
				dbKV.Close()
				return err
			}
			defer dbKV.Close()

			err = dbKV.Update(func(txn *badger.Txn) error {
				opts := badger.DefaultIteratorOptions
				opts.PrefetchSize = 10
				it := txn.NewIterator(opts)
				defer it.Close()
				for it.Rewind(); it.Valid(); it.Next() {
					item := it.Item()
					k := item.Key()
					err := item.Value(func(v []byte) error {

						keeper := false

						for _, v2 := range m {
							m2 := v2.(map[string]interface{})

							if strings.Contains(string(v), m2["source"].(string)) {
								//matched db row
								getColumns := strings.Split(string(v), "|")

								for _, v2 := range getColumns {
									getColumnName := strings.Split(v2, ";")

									if m2["column"].(string) == getColumnName[1] {

										switch m2["operator"].(string) {
										case "Eq":
											if getColumnName[2] == m2["value"].(string) {
												keeper = true
											}
										case "Neq":
											if getColumnName[2] != m2["value"].(string) {
												keeper = true
											}
										case "Greater":
											if getColumnName[2] > m2["value"].(string) {
												keeper = true
											}
										case "GreaterEq":
											if getColumnName[2] >= m2["value"].(string) {
												keeper = true
											}
										case "Less":
											if getColumnName[2] < m2["value"].(string) {
												keeper = true
											}
										case "LessEq":
											if getColumnName[2] <= m2["value"].(string) {
												keeper = true
											}
										case "In":
											// com = series.In
											// extra work in needed!
											// TODO: here
										}

									}

								}
							}

						}

						if !keeper {

							err := txn.Delete([]byte(string(k)))
							if err != nil {
								return err
							}

						}

						return nil
					})
					if err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				return err
			}
		}

	}

	return nil
}

func GetPSColumns(entry Entry) ([]string, error) {

	// connect to database
	// connection string
	psqlconn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		entry.DBHost,
		entry.DBPort,
		entry.DBUser,
		entry.DBPassword,
		entry.DBDatabase)

	// open database
	db, err := sql.Open("postgres", psqlconn)
	if err != nil {
		return nil, err
	}

	// defer close database
	defer db.Close()

	rows, err := db.Query(entry.DBSQL)
	if err != nil {
		return nil, err
	}

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	return cols, nil
}

func GetPSData(entry map[string]interface{}) error {

	dbKV, err := badger.Open(badger.DefaultOptions("tmp/badger"))
	if err != nil {
		dbKV.Close()
		return err
		// log.Fatal(err)
	}
	defer dbKV.Close()
	// dbKV.DropAll()

	// connect to database
	// connection string
	psqlconn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		entry["dbhost"].(string),
		entry["dbport"].(string),
		entry["dbuser"].(string),
		entry["dbpassword"].(string),
		entry["dbdatabase"].(string))

	// open database
	db, err := sql.Open("postgres", psqlconn)
	if err != nil {
		return err
	}

	// defer close database
	defer db.Close()

	rows, err := db.Query(entry["dbsql"].(string))
	if err != nil {
		return err
	}

	// defer close rows
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	colVals := make([]interface{}, len(cols))

	for i := range colVals {
		colVals[i] = new(interface{})
	}

	for rows.Next() {

		counter++

		err = rows.Scan(colVals...)
		if err != nil {
			return err
		}

		colNames, err := rows.Columns()
		if err != nil {
			return err
		}

		var outputter []string

		for idx, name := range colNames {

			a := *colVals[idx].(*interface{})

			if reflect.TypeOf(a) != nil {
				if reflect.TypeOf(a).String() == "[]uint8" {
					outputter = append(outputter, fmt.Sprintf("%v;%v;%v", entry["source"].(string), name, string(a.([]byte))))
				} else {
					outputter = append(outputter, fmt.Sprintf("%v;%v;%v", entry["source"].(string), name, a))
				}
			}

		}

		err = dbKV.Update(func(txn *badger.Txn) error {
			e := badger.NewEntry([]byte(fmt.Sprintf("%02v", counter)), []byte(strings.Join(outputter, "|"))).WithTTL(time.Hour)
			err := txn.SetEntry(e)
			return err
		})
		if err != nil {
			return err
		}

	}

	rows.Close()
	db.Close()

	return nil
}

func GetPSTables(entry EntryDataSource) ([]string, error) {

	// connect to database
	// connection string
	psqlconn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		entry.DBHost,
		entry.DBPort,
		entry.DBUser,
		entry.DBPassword,
		entry.DBDatabase)

	// open database
	db, err := sql.Open("postgres", psqlconn)
	if err != nil {
		return nil, err
	}

	// defer close database
	defer db.Close()

	sql := `
	SELECT distinct tablename 
	FROM pg_catalog.pg_tables
	WHERE schemaname != 'pg_catalog' 
		AND schemaname != 'information_schema'
			and tablename is not null
	order by 1;
			`

	rows, err := db.Query(sql)
	if err != nil {
		return nil, err
	}

	// defer close rows
	defer rows.Close()

	var tables []string

	for rows.Next() {

		var table string

		err = rows.Scan(&table)
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)

	}

	return tables, nil
}
