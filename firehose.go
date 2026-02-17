package main

import (
	"bytes"
	"context"
	"encoding/json"
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

				params := CollectionParameters{
					Context:    ctx,
					AtClient:   atClient,
					RecordCBOR: recordCBOR,
					Event:      evt,
					Logger:     logger,
				}

				collection, _ := splitRecordPath(op.Path)

				var fcmNotifs []FcmNotif

				switch collection {
				case "app.bsky.feed.like":
					notifs, err := HandlePostLike(params)
					if err != nil {
						logger.Error("feed.like collection err", "err", err)
						continue
					}

					if notifs != nil {
						logger.Info("detected like")
						fcmNotifs = append(fcmNotifs, notifs...)
					}
				case "app.bsky.graph.follow":
					notifs, err := HandleFollow(params)
					if err != nil {
						logger.Error("graph.follow collection err", "err", err)
						continue
					}

					if notifs != nil {
						logger.Info("detected follow")
						fcmNotifs = append(fcmNotifs, notifs...)
					}
				case "app.bsky.feed.post":
					notifs, err := HandlePost(params)

					if err != nil {
						logger.Error("feed.post collection err", "err", err)
						continue
					}

					if notifs != nil {
						logger.Info("detected post")
						fcmNotifs = append(fcmNotifs, notifs...)
					}
				case "app.bsky.feed.repost":
					notifs, err := HandleRepost(params)

					if err != nil {
						logger.Error("feed.repost collection err", "err", err)
						continue
					}

					if notifs != nil {
						logger.Info("detected repost")
						fcmNotifs = append(fcmNotifs, notifs...)
					}
				}

				if len(fcmNotifs) > 0 {
					for _, notif := range fcmNotifs {
						SendNotif(notif, fcmClient)
					}
				}

			}
			return nil
		},
	}

	return rsc
}

func splitRecordPath(opPath string) (string, string) {
	split := strings.Split(opPath, "/")

	return split[0], split[1]
}

func HandleFollow(params CollectionParameters) ([]FcmNotif, error) {
	var follow appbsky.GraphFollow
	if err := follow.UnmarshalCBOR(bytes.NewReader(*params.RecordCBOR)); err != nil {
		return nil, err
	}

	// make sure person didnt follow themself
	// (though ngl a notification for this would be funny, maybe unneeded check?)
	if params.Event.Repo != follow.Subject {
		tokens, err := GetPushTokensForDid(follow.Subject)
		if err != nil {
			return nil, err
		}

		if len(tokens) < 1 {
			return nil, nil
		}

		profile, err := appbsky.ActorGetProfile(params.Context, params.AtClient, params.Event.Repo)
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

	if post.Embed != nil && post.Embed.EmbedRecord != nil {
		uri, err := syntax.ParseATURI(post.Embed.EmbedRecord.Record.Uri)
		if err != nil {
			return nil, err
		}

		// is a quote post
		if uri.Collection().String() == "app.bsky.feed.post" {
			parentDid := uri.Authority().DID().String()

			if parentDid == params.Event.Repo {
				return nil, nil
			}

			tokens, err := GetPushTokensForDid(parentDid)
			if err != nil {
				return nil, err
			}

			if len(tokens) < 1 {
				return nil, nil
			}

			var notifs []FcmNotif

			profile, err := appbsky.ActorGetProfile(params.Context, params.AtClient, params.Event.Repo)
			if err != nil {
				return nil, err
			}

			for _, token := range tokens {
				notif := FcmNotif{
					Token: token,
					Notif: &messaging.Notification{
						Title: *profile.DisplayName + " quoted your post",
						Body:  post.Text,
					},
				}
				notifs = append(notifs, notif)
			}

			return notifs, nil
		}
	}

	if post.Reply != nil {
		uri, err := syntax.ParseATURI(post.Reply.Parent.Uri)
		if err != nil {
			return nil, err
		}

		parentDid := uri.Authority().DID().String()

		if parentDid == params.Event.Repo {
			return nil, nil
		}

		tokens, err := GetPushTokensForDid(parentDid)
		if err != nil {
			return nil, err
		}

		if len(tokens) < 1 {
			return nil, nil
		}

		var notifs []FcmNotif

		profile, err := appbsky.ActorGetProfile(params.Context, params.AtClient, params.Event.Repo)
		if err != nil {
			return nil, err
		}

		for _, token := range tokens {
			notif := FcmNotif{
				Token: token,
				Notif: &messaging.Notification{
					Title: *profile.DisplayName + " replied your post",
					Body:  post.Text,
				},
			}
			notifs = append(notifs, notif)
		}

		return notifs, nil
	}

	return nil, nil
}

func HandlePostLike(params CollectionParameters) ([]FcmNotif, error) {
	var like appbsky.FeedLike
	if err := like.UnmarshalCBOR(bytes.NewReader(*params.RecordCBOR)); err != nil {
		return nil, err
	}

	likedPostsDid := strings.Split(like.Subject.Uri, "/")[2]

	// make sure user didnt like their own post
	if params.Event.Repo != likedPostsDid {
		uri, err := syntax.ParseATURI(like.Subject.Uri)
		if err != nil {
			return nil, err
		}

		tokens, err := GetPushTokensForDid(likedPostsDid)
		if err != nil {
			return nil, err
		}

		if len(tokens) < 1 {
			return nil, nil
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
			return nil, err
		}

		var post appbsky.FeedPost
		err = json.Unmarshal(data, &post)
		if err != nil {
			return nil, err
		}

		var notifs []FcmNotif

		profile, err := appbsky.ActorGetProfile(params.Context, params.AtClient, params.Event.Repo)
		if err != nil {
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

		return notifs, nil
	}

	return nil, nil
}

func HandleRepost(params CollectionParameters) ([]FcmNotif, error) {
	var repost appbsky.FeedRepost
	if err := repost.UnmarshalCBOR(bytes.NewReader(*params.RecordCBOR)); err != nil {
		return nil, err
	}

	likedPostsDid := strings.Split(repost.Subject.Uri, "/")[2]

	// make sure user didnt like their own post
	if params.Event.Repo != likedPostsDid {
		uri, err := syntax.ParseATURI(repost.Subject.Uri)
		if err != nil {
			return nil, err
		}

		tokens, err := GetPushTokensForDid(likedPostsDid)
		if err != nil {
			return nil, err
		}

		if len(tokens) < 1 {
			return nil, nil
		}

		record, err := atproto.RepoGetRecord(
			params.Context,
			params.AtClient,
			repost.Subject.Cid,
			uri.Collection().String(),
			uri.Authority().String(),
			uri.RecordKey().String(),
		)

		data, err := json.Marshal(record.Value)
		if err != nil {
			return nil, err
		}

		var post appbsky.FeedPost
		err = json.Unmarshal(data, &post)
		if err != nil {
			return nil, err
		}

		var notifs []FcmNotif

		profile, err := appbsky.ActorGetProfile(params.Context, params.AtClient, params.Event.Repo)
		if err != nil {
			return nil, err
		}

		for _, token := range tokens {
			notif := FcmNotif{
				Token: token,
				Notif: &messaging.Notification{
					Title: *profile.DisplayName + " reposted your post",
					Body:  post.Text,
				},
			}
			notifs = append(notifs, notif)
		}

		return notifs, nil
	}

	return nil, nil
}
