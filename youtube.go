package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"os/user"
	"path/filepath"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/youtube/v3"
)

func getService(ctx context.Context, clientSecretFile, tokenCacheFile string) (*youtube.Service, error) {
	b, err := ioutil.ReadFile(clientSecretFile)
	if err != nil {
		return nil, err
	}

	cfg, err := google.ConfigFromJSON(b, youtube.YoutubeReadonlyScope)
	if err != nil {
		return nil, err
	}

	cl, err := getClient(ctx, cfg, tokenCacheFile)
	if err != nil {
		return nil, err
	}

	svc, err := youtube.New(cl)
	if err != nil {
		return nil, err
	}

	return svc, nil
}

func getClient(ctx context.Context, config *oauth2.Config, tokenCacheFile string) (*http.Client, error) {
	tok, err := tokenFromFile(tokenCacheFile)
	if err != nil {
		tok, err = getTokenFromWeb(config)
		if err != nil {
			return nil, err
		}

		if err := saveToken(tokenCacheFile, tok); err != nil {
			return nil, err
		}
	}

	return config.Client(ctx, tok), nil
}

func getTokenFromWeb(config *oauth2.Config) (*oauth2.Token, error) {
	authURL := config.AuthCodeURL("state-token", oauth2.AccessTypeOffline)

	fmt.Printf("Go to the following link in your browser then type the authorization code: \n%v\n", authURL)

	var code string
	if _, err := fmt.Scan(&code); err != nil {
		return nil, err
	}

	tok, err := config.Exchange(oauth2.NoContext, code)
	if err != nil {
		return nil, err
	}

	return tok, nil
}

func tokenCacheFile() (string, error) {
	usr, err := user.Current()
	if err != nil {
		return "", err
	}

	tokenCacheDir := filepath.Join(usr.HomeDir, ".credentials")

	if err := os.MkdirAll(tokenCacheDir, 0700); err != nil {
		return "", err
	}

	return filepath.Join(tokenCacheDir, url.QueryEscape("ytail.json")), nil
}

func tokenFromFile(file string) (*oauth2.Token, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var t oauth2.Token
	if err := json.NewDecoder(f).Decode(&t); err != nil {
		return nil, err
	}

	return &t, nil
}

func saveToken(file string, token *oauth2.Token) error {
	f, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	if err := json.NewEncoder(f).Encode(token); err != nil {
		return err
	}

	return nil
}
