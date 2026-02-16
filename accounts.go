package main

import (
	"database/sql"
	"errors"
	"log"

	_ "modernc.org/sqlite"
)

func CreateAndOpenAccountDb() *sql.DB {
	db, err := sql.Open("sqlite", "accounts.sqlite")
	if err != nil {
		panic(err)
	}

	defer db.Close()

	_, err = db.Exec(`
CREATE TABLE IF NOT EXISTS
FcmPushTokens (
	fcmToken TEXT,
	did TEXT
);`)

	if err != nil {
		panic(err)
	}
	db.Close()

	db, err = sql.Open("sqlite", "accounts.sqlite")
	if err != nil {
		panic(err)
	}

	return db
}

func RegisterPushToken(fcmToken string, did string) error {
	log.Println("registering fcm token")

	db := CreateAndOpenAccountDb()
	defer db.Close()

	var amount int

	if err := db.QueryRow("SELECT EXISTS(SELECT 1 FROM FcmPushTokens WHERE fcmToken = ? LIMIT 1);", fcmToken).Scan(&amount); err != nil {
		log.Println("Error occurred while attempting to register token")
		return err
	}

	if amount < 1 {
		if _, err := db.Exec("INSERT INTO FcmPushTokens (fcmToken, did) VALUES(?, ?);", fcmToken, did); err != nil {
			log.Println("Error occurred while attempting to register token")
			return err
		}
	} else {
		return errors.New("Fcm token already registered")
	}

	return nil
}

func GetPushTokensForDid(did string) ([]string, error) {
	db := CreateAndOpenAccountDb()
	defer db.Close()

	rows, err := db.Query("SELECT * FROM FcmPushTokens WHERE did = ?", did)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tokens []string

	for rows.Next() {
		var token string
		var uDid string

		if err := rows.Scan(&token, &uDid); err != nil {
			return tokens, err
		}
		tokens = append(tokens, token)
	}
	if err = rows.Err(); err != nil {
		return tokens, err
	}

	return tokens, nil
}
