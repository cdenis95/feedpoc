package model

import "encoding/json"

type Log struct {
	User   string `json:"user"`
	Action string `json:"action"`
	Data   json.RawMessage `json:"data"`
}

type PostCreationLogData struct {
	Id string `json:"id"`
	Type string `json:"type"`
	Post *Post `json:"post"` // TODO In actual project, unmarshal based on
}

type PostEditLogData struct {

}

type PostDeletionLogData struct {
	Id string `json:"id"`
}