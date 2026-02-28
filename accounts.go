package main

import (
	"database/sql"
	"errors"
	"fmt"
	"log"

	_ "modernc.org/sqlite"
)

type FcmToken struct {
	Token    string
	Platform string
	Did      string
}

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
	platform TEXT,
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

func RegisterPushToken(fcmToken string, platform string, did string) error {
	db := CreateAndOpenAccountDb()
	defer db.Close()

	var amount int

	if err := db.QueryRow("SELECT EXISTS(SELECT 1 FROM FcmPushTokens WHERE fcmToken = ? LIMIT 1);", fcmToken).Scan(&amount); err != nil {
		log.Println("Error occurred while attempting to register token")
		return err
	}

	if amount < 1 {
		if _, err := db.Exec("INSERT INTO FcmPushTokens (fcmToken, platform, did) VALUES(?, ?, ?);", fcmToken, platform, did); err != nil {
			log.Println("Error occurred while attempting to register token")
			return err
		}

		fmt.Println("[register push] registered new push token")
	} else {
		return errors.New("Fcm token already registered")
	}

	return nil
}

func GetPushTokensForDid(did string) ([]FcmToken, error) {
	db := CreateAndOpenAccountDb()
	defer db.Close()

	rows, err := db.Query("SELECT * FROM FcmPushTokens WHERE did = ?", did)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tokens []FcmToken

	for rows.Next() {
		var token string
		var platform string
		var uDid string

		if err := rows.Scan(&token, &platform, &uDid); err != nil {
			return tokens, err
		}

		fcmToken := FcmToken{
			Token:    token,
			Platform: platform,
			Did:      uDid,
		}

		tokens = append(tokens, fcmToken)
	}
	if err = rows.Err(); err != nil {
		return tokens, err
	}

	return tokens, nil
}
