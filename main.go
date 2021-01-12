package main

import (
	"bufio"
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"os"
	"strings"
)

type user struct {
	name  string
	email string
	ip    string
}

func openConnection() (*sql.DB, error) {
	return sql.Open("sqlite3", "user.db")
}

func mountDatabase() (string, error) {
	os.Remove("user.db")

	db, err := openConnection()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	stmt := `
		create table if not exists user (
			id integer not null primary key, 
			name varchar(255),
			email varchar(100),
			ip varchar(15)
		);
	`
	_, err = db.Exec(stmt)
	if err != nil {
		log.Fatal(err)
	}

	return "--> Db montado com sucesso!", err
}

func readCsvFile() []user {
	file, err := os.Open("file.csv")
	if err != nil {
		panic(err)
	}

	scanner := bufio.NewScanner(file)

	var users []user
	for scanner.Scan() {
		line := scanner.Text()
		items := strings.Split(line, ",")

		var u user
		u.name = fmt.Sprintf("%s %s", items[1], items[2])
		u.email = items[3]
		u.ip = items[5]

		users = append(users, u)
	}

	return users
}

func main() {
	msg, err := mountDatabase()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(msg)

	db, err := openConnection()
	defer db.Close()

	users := readCsvFile()

	fmt.Println("--> Preparando para realizar integração.")
	for _, user := range users {
		stmt, _ := db.Prepare("insert into user (name, email, ip) values (?, ?, ?)")
		stmt.Exec(user.name, user.email, user.ip)
	}

	fmt.Println("--> Integração realizada com sucesso!")
}
