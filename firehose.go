package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"strings"

	"firebase.google.com/go/messaging"
	"github.com/bluesky-social/indigo/api/atproto"
	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/events"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/indigo/repo"
	"github.com/bluesky-social/indigo/xrpc"
)

type CollectionParameters struct {
	Context    context.Context
	AtClient   *xrpc.Client
	RecordCBOR *[]byte
	Event      *atproto.SyncSubscribeRepos_Commit
	Logger     *slog.Logger
}

func ConsumeFirehose(ctx context.Context, atClient *xrpc.Client, fcmClient *messaging.Client, logger *slog.Logger) *events.RepoStreamCallbacks {
	log.Println("listening to firehose")

	rsc := &events.RepoStreamCallbacks{
		RepoCommit: func(evt *atproto.SyncSubscribeRepos_Commit) error {
			for _, op := range evt.Ops {
				// only looking at record creation events
				if op.Action != "create" {
					continue
				}

				rr, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(evt.Blocks))
				if err != nil {
					logger.Error("failed to read repo from car", "err", err)
					return nil
				}

				// read the record bytes from blocks, and verify CID
				rc, recordCBOR, err := rr.GetRecordBytes(ctx, op.Path)
				if err != nil {
					logger.Error("reading record from event blocks (CAR)", "err", err)
					continue
				}
				if op.Cid == nil || lexutil.LexLink(rc) != *op.Cid {
					logger.Error("mismatch between commit op CID and record block", "recordCID", rc, "opCID", op.Cid)
					continue
				}

				_, err = getOperationType(*recordCBOR)
				if err != nil {
					logger.Info("decode err", "err", err)
				}
			}
			return nil
		},
	}

	return rsc
}

func getOperationType(recordCBOR []byte) (any, error) {
	recordType, err := lexutil.TypeExtract(recordCBOR)
	if err != nil {
		return nil, err
	}

	fmt.Println("type: " + recordType)
	return nil, nil
}

func HandleFollow(params CollectionParameters) ([]FcmNotif, error) {
	var follow appbsky.GraphFollow
	if err := follow.UnmarshalCBOR(bytes.NewReader(*params.RecordCBOR)); err != nil {
		return nil, err
	}

	profile, err := appbsky.ActorGetProfile(params.Context, params.AtClient, params.Event.Repo)
	if err != nil {
		return nil, err
	}

	// make sure person didnt follow themself
	// (though ngl a notification for this would be funny, maybe unneeded check?)
	if params.Event.Repo != follow.Subject {
		tokens, err := GetPushTokensForDid(follow.Subject)
		if err != nil {
			return nil, err
		}

		var notifs []FcmNotif

		for _, token := range tokens {
			notif := FcmNotif{
				Token: token,
				Notif: &messaging.Notification{
					Title: "New follower!",
					Body:  *profile.DisplayName,
				},
			}
			notifs = append(notifs, notif)
		}

		return notifs, nil
	}

	return nil, nil
}

func HandlePost(params CollectionParameters) ([]FcmNotif, error) {
	var post appbsky.FeedPost
	if err := post.UnmarshalCBOR(bytes.NewReader(*params.RecordCBOR)); err != nil {
		return nil, err
	}

	return nil, nil
}

func HandlePostLike(params CollectionParameters) ([]FcmNotif, error) {
	var like appbsky.FeedLike
	if err := like.UnmarshalCBOR(bytes.NewReader(*params.RecordCBOR)); err != nil {
		return nil, err
	}

	likedPostsDid := strings.Split(like.Subject.Uri, "/")[2]

	if likedPostsDid == "did:plc:tshzimytn4vesorvxd45kjn7" {
		fmt.Println("asuiofjodsfjigojid")
	}

	// make sure user didnt like their own post
	if params.Event.Repo != likedPostsDid {
		uri, err := syntax.ParseATURI(like.Subject.Uri)
		if err != nil {
			fmt.Println("ok")
			return nil, err
		}

		record, err := atproto.RepoGetRecord(
			params.Context,
			params.AtClient,
			like.Subject.Cid,
			uri.Collection().String(),
			uri.Authority().String(),
			uri.RecordKey().String(),
		)

		data, err := json.Marshal(record.Value)
		if err != nil {
			fmt.Println("ok 2")
			return nil, err
		}

		var post appbsky.FeedPost
		err = json.Unmarshal(data, &post)
		if err != nil {
			fmt.Println("ok 3")
			return nil, err
		}

		if likedPostsDid == "did:plc:tshzimytn4vesorvxd45kjn7" {
			fmt.Println("detected test did")
		}

		var notifs []FcmNotif
		tokens, err := GetPushTokensForDid(likedPostsDid)
		if err != nil {
			return nil, err
		}

		if len(tokens) > 0 {
			fmt.Println(" - detected post like for an account; sending notification")

			profile, err := appbsky.ActorGetProfile(params.Context, params.AtClient, params.Event.Repo)
			if err != nil {
				fmt.Println("ok 4")
				return nil, err
			}

			for _, token := range tokens {
				notif := FcmNotif{
					Token: token,
					Notif: &messaging.Notification{
						Title: *profile.DisplayName + " liked your post",
						Body:  post.Text,
					},
				}
				notifs = append(notifs, notif)
			}
		}

		return notifs, nil
	} else {
		fmt.Println("is same")
	}

	return nil, nil
}
