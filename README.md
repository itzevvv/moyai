todo: add more info


this is an attempt at writing a notification service for bluesky, specifically as a drop-in replacement for [social-app](https://github.com/bluesky-social/social-app). code isnt pretty, still needs some cleanup and further work.

currently implemented:
- follow
- likes
- replies
- reposts
- quote posts
- mentions
- reposts of reposts
- likers on reposts

still need to handle:
- propogate (?) notifs through a reply chain
- starterpack join notifs
- verification + unverification

todo:
- auth
- sync notification preferences (requires auth)
- chat msg notifications (requires auth)
- probably have some kind of notification send rate limit
- actually have the notifications take you to the notification page / posts
