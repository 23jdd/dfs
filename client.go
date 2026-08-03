package dfs

import (
	"github.com/23jdd/mrpc"
)

type Client struct {
	c *mrpc.Client
}

func NewClient(address string, port int) *Client {
	return &Client{
		c: mrpc.NewClient(address, port),
	}
}
