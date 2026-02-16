package main

import (
	"context"
	"fmt"

	"firebase.google.com/go/messaging"
)

type FcmNotif struct {
	Token string
	Notif *messaging.Notification
}

func SendNotif(notif FcmNotif, fcmClient *messaging.Client) {
	// return if notification invalid
	if notif.Notif.Title == "" || notif.Notif.Body == "" {
		return
	}

	_, err := fcmClient.Send(context.Background(), &messaging.Message{
		Notification: notif.Notif,
		Token:        notif.Token,
	})

	if err != nil {
		fmt.Printf("error sending notif: %v", err)
		return
	}

	fmt.Println("sent test notif!!!!!!!!!")
}
