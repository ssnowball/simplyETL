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

type Mapper struct {
	Key  []byte
	Data []byte
}

type GroupBy struct {
	Option  string
	Outcome float64
}

var counter int

// var dbKV *badger.DB

func RunDataChange(command interface{}) (err error) {

	option := reflect.TypeOf(command).String()

	if option == "map[string]interface {}" {
		m := command.(map[string]interface{})

		switch m["code"] {
		case "#00#":

			// get the data

			if m["dataconnection"].(string) == "MS" {
				// dfs[m["source"].(string)] = df
			} else {
				err = GetPSData(m)
			}

			if err != nil {
				return err
			}

		case "#01#":

			// JOIN

			tempTable := make(map[string][]byte)

			dbKV, err := badger.Open(badger.DefaultOptions("tmp/badger"))
			if err != nil {
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

								if colData[0] == m["columna"].(string) {

									for _, v3 := range tempTable {

										// need 2nd table in string
										if strings.Contains(string(v3), fmt.Sprintf("%v;%v", m["columnb"].(string), colData[1])) {
											matchedCouner++
											oldIndex := strings.Split(string(k), ".")
											// e := badger.NewEntry([]byte(fmt.Sprintf("%v.%02v", oldIndex[0], matchedCouner)), []byte(fmt.Sprintf("%v|%v|%v;%v-on-%v", string(v), string(v3), m["source"].(string), m["columna"].(string), m["columnb"].(string)))).WithTTL(time.Hour)
											e := badger.NewEntry([]byte(fmt.Sprintf("%v.%02v", oldIndex[0], matchedCouner)), []byte(fmt.Sprintf("%v|%v", string(v), string(v3)))).WithTTL(time.Hour)
											err = txn.SetEntry(e)
											if err != nil {
												return err
											}
										}
									}

									// type of join determines what gets deleted

									switch m["join"].(string) {
									case "Inner":

										// fmt.Println("HERE--------------------")
										// fmt.Println(matchedCouner)
										// fmt.Println(string(k))

										// delete row a if no match
										// if matchedCouner == 0 {
										// 	err := txn.Delete([]byte(string(k)))
										// 	if err != nil {
										// 		return err
										// 	}
										// }

										if matchedCouner > 0 {
											err := txn.Delete([]byte(string(k)))
											if err != nil {
												return err
											}
										}

									case "Left":
										//TODO :: check if correct if
										// delete old row on match to leave new row
										if matchedCouner > 0 {
											err := txn.Delete([]byte(string(k)))
											if err != nil {
												return err
											}
										}

										// if matchedCouner == 0 {
										// 	err := txn.Delete([]byte(string(k)))
										// 	if err != nil {
										// 		return err
										// 	}
										// }
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

			return nil

		case "#04#":

			// FILTER

			dbKV, err := badger.Open(badger.DefaultOptions("tmp/badger"))
			if err != nil {
				// dbKV.Close()
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

						for _, v2 := range m["filters"].([]interface{}) {
							m2 := v2.(map[string]interface{})

							if strings.Contains(string(v), m2["column"].(string)) {
								//matched db row
								getColumns := strings.Split(string(v), "|")

								for _, v3 := range getColumns {

									getColumnName := strings.Split(v3, ";")

									if m2["column"].(string) == getColumnName[0] {

										switch m2["operator"].(string) {
										case "Eq":
											if getColumnName[1] == m2["value"].(string) {
												keeper = true
											}
										case "Neq":
											if getColumnName[1] != m2["value"].(string) {
												keeper = true
											}
										case "Greater":
											if getColumnName[1] > m2["value"].(string) {
												keeper = true
											}
										case "GreaterEq":
											if getColumnName[1] >= m2["value"].(string) {
												keeper = true
											}
										case "Less":
											if getColumnName[1] < m2["value"].(string) {
												keeper = true
											}
										case "LessEq":
											if getColumnName[1] <= m2["value"].(string) {
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

			// GROUP BY

			cols := m["groupcolumns"].([]interface{})
			aggs := m["aggs"].([]interface{})
			colS := make(map[string]int)
			groupList := make(map[string]map[string]float64)
			colCheck := 0

			for _, v := range cols[0].([]interface{}) {
				colS[v.(string)] = 1
				colCheck++
			}

			dbKV, err := badger.Open(badger.DefaultOptions("tmp/badger"))
			if err != nil {
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
					var newData string
					var groupListString []string

					err := item.Value(func(v []byte) error {

						keepRow := 0
						//matched db row
						getColumns := strings.Split(string(v), "|")

						for _, v2 := range getColumns {
							getColumnName := strings.Split(v2, ";")

							_, keepCol := colS[getColumnName[0]]
							if keepCol {
								groupListString = append(groupListString, v2)
								keepRow++
							}
						}

						groupList[strings.Join(groupListString, "|")] = make(map[string]float64)

						if keepRow == colCheck {
							newData = fmt.Sprintf("%v|GROUP;Y", string(v))
						} else {
							newData = fmt.Sprintf("%v|GROUP;N", string(v))
						}

						return nil
					})
					if err != nil {
						return err
					}

					e := badger.NewEntry(k, []byte(newData)).WithTTL(time.Hour)
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

			fmt.Println("---------FIRST LOOP---------------")
			fmt.Println(groupList)

			for iG := range groupList {

				fmt.Println("-----------FIRST INNER LOOP-------------")
				fmt.Println(iG)

				err = dbKV.Update(func(txn *badger.Txn) error {
					opts := badger.DefaultIteratorOptions
					opts.PrefetchSize = 10
					it := txn.NewIterator(opts)
					defer it.Close()
					for it.Rewind(); it.Valid(); it.Next() {
						item := it.Item()
						// k := item.Key()
						// var newData string
						var groupListString []string

						err := item.Value(func(v []byte) error {

							// keepRow := 0
							//matched db row
							getColumns := strings.Split(string(v), "|")

							for _, v2 := range getColumns {
								// getColumnName := strings.Split(v2, ";")

								// _, keepCol := colS[getColumnName[0]]
								if strings.Contains(iG, v2) {
									groupListString = append(groupListString, v2)
									// keepRow++
								}
							}

							newChc := strings.Join(groupListString, "|")

							if iG == newChc {
								fmt.Println("-----FOUND ROW TO ADD TO----------")
								fmt.Println(iG)
								fmt.Println(newChc)

								for _, v := range aggs {

									fmt.Println("------------AGG LOOP----------")

									v2 := v.(map[string]interface{})

									columnNameNew := fmt.Sprintf("%v.%v", v2["column"], v2["agtype"])

									for _, v3 := range getColumns {

										getColumnName := strings.Split(v3, ";")

										if v2["column"].(string) == getColumnName[0] {

											val, _ := strconv.ParseFloat(getColumnName[1], 64)

											switch v2["agtype"].(string) {
											case "MAX":
												old := groupList[newChc][columnNameNew]
												if val > old {
													groupList[newChc][columnNameNew] = val
												}
											case "MIN":
												old := groupList[newChc][columnNameNew]
												if val < old {
													groupList[newChc][columnNameNew] = val
												}
											case "SUM":
												// fmt.Println("MATCHED SUM-------------------")
												// groupList[newChc]["SUM"] += val
												groupList[newChc][columnNameNew] += val
											case "COUNT":
												// fmt.Println("------COUNT ADDD------------")
												// fmt.Println(newChc)
												// fmt.Printf("%v.%v\n", v2["column"], v2["agtype"])
												groupList[newChc][columnNameNew]++

											}
										}

									}

									// err = dbKV.Update(func(txn *badger.Txn) error {
									// 	opts := badger.DefaultIteratorOptions
									// 	opts.PrefetchSize = 10
									// 	it := txn.NewIterator(opts)
									// 	defer it.Close()
									// 	for it.Rewind(); it.Valid(); it.Next() {
									// 		item := it.Item()
									// 		// k := item.Key()
									// 		err := item.Value(func(v []byte) error {
									// 			//matched db row

									// 			if strings.Contains(string(v), "GROUP;Y") {

									// 				getColumns := strings.Split(string(v), "|")

									// 				for _, v3 := range getColumns {

									// 					getColumnName := strings.Split(v3, ";")

									// 					if v2["column"].(string) == getColumnName[0] {

									// 						val, _ := strconv.ParseFloat(getColumnName[1], 64)

									// 						switch v2["agtype"].(string) {
									// 						case "MAX":
									// 							old := groupList[newChc][columnNameNew]
									// 							if val > old {
									// 								groupList[newChc][columnNameNew] = val
									// 							}
									// 						case "MIN":
									// 							old := groupList[newChc][columnNameNew]
									// 							if val < old {
									// 								groupList[newChc][columnNameNew] = val
									// 							}
									// 						case "SUM":
									// 							// fmt.Println("MATCHED SUM-------------------")
									// 							// groupList[newChc]["SUM"] += val
									// 							groupList[newChc][columnNameNew] += val
									// 						case "COUNT":
									// 							fmt.Println("------COUNT ADDD------------")
									// 							fmt.Println(newChc)
									// 							// fmt.Printf("%v.%v\n", v2["column"], v2["agtype"])
									// 							groupList[newChc][columnNameNew]++

									// 						}
									// 					}

									// 				}
									// 			}

									// 			return nil
									// 		})
									// 		if err != nil {
									// 			return err
									// 		}

									// 	}
									// 	return nil
									// })
									// if err != nil {
									// 	return err
									// }

								}

							}

							// groupList[strings.Join(groupListString, "|")] = make(map[string]float64)

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

				// for _, v := range aggs {
				// 	v2 := v.(map[string]interface{})

				// 	columnNameNew := fmt.Sprintf("%v.%v", v2["column"], v2["agtype"])

				// 	err = dbKV.Update(func(txn *badger.Txn) error {
				// 		opts := badger.DefaultIteratorOptions
				// 		opts.PrefetchSize = 10
				// 		it := txn.NewIterator(opts)
				// 		defer it.Close()
				// 		for it.Rewind(); it.Valid(); it.Next() {
				// 			item := it.Item()
				// 			// k := item.Key()
				// 			err := item.Value(func(v []byte) error {
				// 				//matched db row

				// 				if strings.Contains(string(v), "GROUP;Y") {

				// 					getColumns := strings.Split(string(v), "|")

				// 					for _, v3 := range getColumns {

				// 						getColumnName := strings.Split(v3, ";")

				// 						if v2["column"].(string) == getColumnName[0] {

				// 							val, _ := strconv.ParseFloat(getColumnName[1], 64)

				// 							switch v2["agtype"].(string) {
				// 							case "MAX":
				// 								old := groupList[iG][columnNameNew]
				// 								if val > old {
				// 									groupList[iG][columnNameNew] = val
				// 								}
				// 							case "MIN":
				// 								old := groupList[iG][columnNameNew]
				// 								if val < old {
				// 									groupList[iG][columnNameNew] = val
				// 								}
				// 							case "SUM":
				// 								// fmt.Println("MATCHED SUM-------------------")
				// 								// groupList[iG]["SUM"] += val
				// 								groupList[iG][columnNameNew] += val
				// 							case "COUNT":
				// 								// fmt.Println("------COUNT ADDD------------")
				// 								// fmt.Println(v2)
				// 								// fmt.Printf("%v.%v\n", v2["column"], v2["agtype"])
				// 								groupList[iG][columnNameNew]++

				// 							}
				// 						}

				// 					}
				// 				}

				// 				return nil
				// 			})
				// 			if err != nil {
				// 				return err
				// 			}

				// 		}
				// 		return nil
				// 	})
				// 	if err != nil {
				// 		return err
				// 	}

				// }

			}

			fmt.Println("-----------END AGG LOOP------------")
			fmt.Println(groupList)

			nextIdex := 0

			// DELETE LOOP
			err = dbKV.Update(func(txn *badger.Txn) error {
				opts := badger.DefaultIteratorOptions
				opts.PrefetchSize = 10
				it := txn.NewIterator(opts)
				defer it.Close()
				for it.Rewind(); it.Valid(); it.Next() {
					item := it.Item()
					k := item.Key()
					err := item.Value(func(v []byte) error {

						nextIdexID := strings.Split(string(k), ".")
						counterB, _ := strconv.Atoi(nextIdexID[0])

						if counterB > nextIdex {
							nextIdex = counterB
						}

						if strings.Contains(string(v), "GROUP;Y") {
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
				return nil
			})

			// counterA := nextIdex + 1
			nextIdex++

			for i, v := range groupList {

				// fmt.Println("REBUILD----------------------")
				// fmt.Println(i)
				// fmt.Println(v)

				// colSplit := strings.Split(i, ";")

				var newColumns string
				var newColumnsBuild []string

				for i2, v2 := range v {
					// newColumnsBuild = append(newColumnsBuild, fmt.Sprintf("%v.%v;%v", colSplit[0], i2, v2))
					newColumnsBuild = append(newColumnsBuild, fmt.Sprintf("%v;%v", i2, v2))
				}

				newColumns = strings.Join(newColumnsBuild, "|")

				err = dbKV.Update(func(txn *badger.Txn) error {
					// e := badger.NewEntry([]byte(fmt.Sprintf("%02v.00", counterA)), []byte(fmt.Sprintf("%v|%v", i, newColumns))).WithTTL(time.Hour)
					e := badger.NewEntry([]byte(fmt.Sprintf("%02v.00", nextIdex)), []byte(fmt.Sprintf("%v|%v", i, newColumns))).WithTTL(time.Hour)
					err := txn.SetEntry(e)
					return err
				})
				if err != nil {
					return err
				}

				// counterA++
				nextIdex++
			}

		case "#10#":

			// SELECT

			cols := m["columns"].([]interface{})
			colS := make(map[string]int)
			for _, v := range cols[0].([]interface{}) {
				colS[v.(string)] = 1
			}

			dbKV, err := badger.Open(badger.DefaultOptions("tmp/badger"))
			if err != nil {
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

						//matched db row
						getColumns := strings.Split(string(v), "|")

						for _, v2 := range getColumns {

							getColumnName := strings.Split(v2, ";")

							_, keepCol := colS[getColumnName[0]]
							if keepCol {
								outputter = append(outputter, v2)
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

			// ARRANGE

			dbKV, err := badger.Open(badger.DefaultOptions("tmp/badger"))
			if err != nil {
				return err
			}
			defer dbKV.Close()

			tempTable2 := make(map[string]Mapper)

			err = dbKV.View(func(txn *badger.Txn) error {
				opts := badger.DefaultIteratorOptions
				opts.PrefetchSize = 10
				it := txn.NewIterator(opts)
				defer it.Close()
				for it.Rewind(); it.Valid(); it.Next() {
					item := it.Item()
					k := item.Key()
					err := item.Value(func(v []byte) error {

						if strings.Contains(string(v), m["column"].(string)) {

							getColumns := strings.Split(string(v), "|")
							var index string

							for _, v2 := range getColumns {
								getColumnName := strings.Split(v2, ";")

								if getColumnName[0] == m["column"].(string) {
									index = getColumnName[1]

								}

							}

							tempTable2[index] = Mapper{
								Key:  k,
								Data: v,
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

			keys2 := make([]string, 0, len(tempTable2))
			for k := range tempTable2 {
				keys2 = append(keys2, k)
			}

			switch m["direction"].(string) {
			case "ASC":
				sort.Strings(keys2)
			case "DESC":
				sort.Sort(sort.Reverse(sort.StringSlice(keys2)))
			}

			for i, k := range keys2 {
				err = dbKV.Update(func(txn *badger.Txn) error {

					e := badger.NewEntry([]byte(fmt.Sprintf("%02v.00", i+1)), tempTable2[k].Data).WithTTL(time.Hour)
					err := txn.SetEntry(e)
					if err != nil {
						return err
					}

					err = txn.Delete([]byte(tempTable2[k].Key))
					if err != nil {
						return err
					}

					return err
				})
				if err != nil {
					return err
				}

			}

		case "#12#":

			// SUBSET

			dbKV, err := badger.Open(badger.DefaultOptions("tmp/badger"))
			if err != nil {
				return err
			}
			defer dbKV.Close()

			var rowIndexer []string

			err = dbKV.View(func(txn *badger.Txn) error {
				opts := badger.DefaultIteratorOptions
				opts.PrefetchValues = false
				it := txn.NewIterator(opts)
				defer it.Close()
				for it.Rewind(); it.Valid(); it.Next() {
					item := it.Item()
					k := item.Key()
					rowIndexer = append(rowIndexer, string(k))
				}
				return nil
			})
			if err != nil {
				return err
			}

			var rowsS []string

			numRows, err := strconv.Atoi(m["value"].(string))
			if err != nil {
				return err
			}

			sort.Strings(rowIndexer)

			if m["direction"].(string) == "First" {

				rowsS = rowIndexer[0:numRows]
				// for i := 0; i < numRows; i++ {
				// 	// rowsS = append(rowsS, fmt.Sprintf("%02v", i+1))
				// }
			} else {
				// need to reverse counter
				lower := len(rowIndexer) - numRows
				rowsS = rowIndexer[lower:]

				// for i := lower; i < maxRows; i++ {
				// 	// rows[fmt.Sprintf("%02v", i+1)] = fmt.Sprintf("%02v", i+1)
				// 	rowsS = append(rowsS, fmt.Sprintf("%02v", i+1))
				// }
			}

			err = dbKV.Update(func(txn *badger.Txn) error {
				opts := badger.DefaultIteratorOptions
				opts.PrefetchValues = false
				it := txn.NewIterator(opts)
				defer it.Close()
				for it.Rewind(); it.Valid(); it.Next() {
					item := it.Item()
					k := item.Key()

					keepCol := false

					for _, v := range rowsS {
						if strings.Contains(string(k), v) {
							keepCol = true
						}

					}
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

			//RENAME

			dbKV, err := badger.Open(badger.DefaultOptions("tmp/badger"))
			if err != nil {
				// dbKV.Close()
				return err
			}
			defer dbKV.Close()

			// fmt.Println("RENAME -------------------- HERE")

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

						if strings.Contains(string(v), m["column"].(string)) {
							//matched db row

							getColumns := strings.Split(string(v), "|")

							for _, v2 := range getColumns {
								getColumnName := strings.Split(v2, ";")

								if getColumnName[0] == m["column"].(string) {
									newName := strings.Split(getColumnName[0], ".")
									newName[2] = m["value"].(string)

									outputter = append(outputter, fmt.Sprintf("%v;%v", strings.Join(newName, "."), getColumnName[1]))
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

	// attemps := 3

	// var dbKV *badger.DB

	// for i := 0; i < attemps; i++ {

	// 	fmt.Printf("attempt: %v ----------------------------\n", i)

	// 	dbKV, err := badger.Open(badger.DefaultOptions("tmp/badger"))
	// 	if err != nil {
	// 		time.Sleep(1 * time.Second)
	// 	} else {
	// 		defer dbKV.Close()
	// 		break

	// 	}

	// }

	dbKV, err := badger.Open(badger.DefaultOptions("tmp/badger"))
	if err != nil {
		// dbKV.Close()
		fmt.Println(err)
		return err
		// log.Fatal(err)
	}
	defer dbKV.Close()

	fmt.Println("here1")

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
					outputter = append(outputter, fmt.Sprintf("%v.%v;%v", entry["source"].(string), name, string(a.([]byte))))
				} else {
					outputter = append(outputter, fmt.Sprintf("%v.%v;%v", entry["source"].(string), name, a))
				}
			}

		}

		err = dbKV.Update(func(txn *badger.Txn) error {
			e := badger.NewEntry([]byte(fmt.Sprintf("%02v.00", counter)), []byte(strings.Join(outputter, "|"))).WithTTL(time.Hour)
			err := txn.SetEntry(e)
			return err
		})
		if err != nil {
			return err
		}

	}

	dbKV.Close()

	fmt.Println(dbKV)

	rows.Close()
	db.Close()

	fmt.Println("here2")

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
