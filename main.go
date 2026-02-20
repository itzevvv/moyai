package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
	"strings"

	"github.com/bluesky-social/indigo/atproto/auth"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/sequential"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/gorilla/websocket"
	"google.golang.org/api/option"

	firebase "firebase.google.com/go"
)

type RegisterPushNotificationData struct {
	ServiceDid    string `json:"serviceDid"`
	Platform      string `json:"platform"`
	Token         string `json:"token"`
	AppId         string `json:"appId"`
	AgeRestricted bool   `json:"ageRestricted"`
}

func VerifyBlueskyJWT(ctx context.Context, rawJWT string) (string, error) {
	// Use the default PLC/identity resolver directory
	dir := identity.DefaultDirectory()

	validator := auth.ServiceAuthValidator{
		Audience:        "did:web:atproto.chattest.mrrp.lol",
		Dir:             dir,
		TimestampLeeway: 0,
	}

	did, err := validator.Validate(ctx, rawJWT, nil)
	if err != nil {
		return "", err
	}

	return did.String(), nil
}

func XrpcRegisterPushNotifications(w http.ResponseWriter, req *http.Request) {
	bytes, err := io.ReadAll(req.Body)
	if err != nil {
		fmt.Println("ReadAll error:", err)
	}

	// read the bearer token so we know the account associated with the request
	bearerToken := strings.Split(req.Header["Authorization"][0], " ")[1]

	did, err := VerifyBlueskyJWT(context.Background(), bearerToken)
	if err != nil {
		fmt.Println("err:", err)
		return
	}

	var notif RegisterPushNotificationData
	err = json.Unmarshal(bytes, &notif)
	if err != nil {
		fmt.Println("err unmarshaling json:", err)
		return
	}

	RegisterPushToken(notif.Token, did)
}

func XrpcUnregisterPushNotifications(w http.ResponseWriter, req *http.Request) {
	bytes, err := io.ReadAll(req.Body)
	if err != nil {
		fmt.Println("ReadAll error:", err)
	}

	fmt.Printf("[unregister push] received: %s\n", string(bytes))
}

func main() {
	fmt.Println(" === Moyai Notification Service ===")

	http.HandleFunc("/xrpc/app.bsky.notification.registerPush", XrpcRegisterPushNotifications)
	http.HandleFunc("/xrpc/app.bsky.notification.unregisterPush", XrpcUnregisterPushNotifications)

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	uri := "wss://relay1.us-east.bsky.network/xrpc/com.atproto.sync.subscribeRepos"
	con, _, err := websocket.DefaultDialer.Dial(uri, http.Header{})
	if err != nil {
		fmt.Println("error connecting to websocket:", err)
		return
	}

	ctx := context.Background()
	atClient := &xrpc.Client{
		Host: "https://public.api.bsky.app",
	}

	opt := option.WithCredentialsFile("serviceAccountKey.json")

	app, err := firebase.NewApp(context.Background(), nil, opt)

	if err != nil {
		fmt.Println("error initializing app:", err)
		return
	}

	fcmClient, err := app.Messaging(context.Background())
	if err != nil {
		fmt.Println("error creating client:", err)

		return
	}

	rsc := ConsumeFirehose(ctx, atClient, fcmClient, logger)

	sched := sequential.NewScheduler("firehose", rsc.EventHandler)
	go events.HandleRepoStream(context.Background(), con, sched, logger)

	log.Println("http server starting on :2845")
	log.Fatal(http.ListenAndServe(":2845", nil))

}
