package main

import (
	"context"
	"fmt"

	"firebase.google.com/go/messaging"
)

type FcmNotif struct {
	Token FcmToken

	Title  string
	Body   string
	Reason string
	Url    string
}

func SendNotif(notif FcmNotif, fcmClient *messaging.Client) {
	// return if notification invalid
	if notif.Title == "" {
		return
	}

	_, err := fcmClient.Send(context.Background(), &messaging.Message{
		Notification: &messaging.Notification{
			Title: notif.Title,
			Body:  notif.Body,
		},
		Token: notif.Token.Token,
	})

	if err != nil {
		fmt.Printf("error sending notif: %v\n", err)
		// todo: handle registration-token-not-registered

		return
	}

	fmt.Println("[firebase] firebase notification sent")
}
