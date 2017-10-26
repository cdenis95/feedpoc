package model

type Post struct {
	Id string `bson:"_id"`
	Type string
	Title string
	Description string
}
