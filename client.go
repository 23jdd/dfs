package dfs

import (
	"github.com/23jdd/mrpc"
)

type Client struct {
	mc *mrpc.Client
}

func NewClient(address string, port int) *Client {
	return &Client{mc: mrpc.NewClient(address, port)}
}
func (cli *Client) Read(filename string, offset int, length int) {

}
func (cli *Client) Write(filename string, offset int, buf []byte) {

}
func (cli *Client) GetInfo(filename string) (*GetFileInfoReply, error) {
	req := &GetFileInfoRequest{
		Path: filename,
	}
	rep := &GetFileInfoReply{}
	err := cli.mc.Call(MasterSerivceName+"."+GetFileInfo, req, rep)
	if err != nil {
		return nil, err
	}
	return rep, nil
}
func (cli *Client) Create(filename string) error {
	req := &CreateFileRequest{
		Path: filename,
	}
	rep := &CreateFileReply{}
	return cli.mc.Call(MasterSerivceName+"."+CreateFile, req, rep)
}
func (cli *Client) Delete(filename string) error {
	req := &DeleteFileRequest{
		Path: filename,
	}
	rep := &DeleteFileReply{}
	return cli.mc.Call(MasterSerivceName+"."+DeleteFile, req, rep)
}
func (cli *Client) Close() {
	cli.mc.Close()
}
