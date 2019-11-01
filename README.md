# ytail

there's a lot of stuff getting uploaded to youtube. there's a lot of stuff
getting deleted from youtube. how cool would it be if you could see some of
that deleted stuff after the fact?

## get started

1. install go, sqlite, and beanstalkd
3. `go get github.com/deoxxa/ytail`
4. `cd $(go env GOPATH)/src/github.com/deoxxa/ytail`
2. get a youtube API key (figure it out) and put it in `client_secret.json`
5. `$EDITOR ytail.toml` (add `[search.xxx]` entries, it should be easy)
6. `./ytail`
7. `www-browser http://127.0.0.1:8000/removed`
8. wait a while, hit f5 every now and then

## license

you're a license
