package main

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/yedf/dtmcli"
	"github.com/yedf/dtmgrpc"
	"github.com/yedf/dtmgrpc-go-sample/busi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DtmGrpcServer dtm grpc service address
const DtmGrpcServer = "localhost:58080"

// TransReq transaction request payload
type TransReq struct {
	Amount         int    `json:"amount"`
	TransInResult  string `json:"transInResult"`
	TransOutResult string `json:"transOutResult"`
}

func (t *TransReq) String() string {
	return fmt.Sprintf("amount: %d transIn: %s transOut: %s", t.Amount, t.TransInResult, t.TransOutResult)
}

// BusiGrpcPort 1
const BusiGrpcPort = 50581

// BusiGrpc busi service grpc address
var BusiGrpc string = fmt.Sprintf("localhost:%d", BusiGrpcPort)

// DtmClient grpc client for dtm
var DtmClient dtmgrpc.DtmClient = nil

func handleGrpcBusiness(in *dtmgrpc.BusiRequest, result1 string, result2 string, busi string) error {
	res := dtmcli.OrString(result1, result2, dtmcli.ResultSuccess)
	dtmcli.Logf("grpc busi %s %s result: %s", busi, in.Info, res)
	if res == dtmcli.ResultSuccess {
		return nil
	} else if res == dtmcli.ResultFailure {
		return status.New(codes.Aborted, "user want to rollback").Err()
	}
	return status.New(codes.Internal, fmt.Sprintf("unknow result %s", res)).Err()
}

// busiServer is used to implement helloworld.GreeterServer.
type busiServer struct {
	busi.UnimplementedBusiServer
}

// GrpcStartup for grpc
func GrpcStartup() {
	conn, err := grpc.Dial(DtmGrpcServer, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithUnaryInterceptor(dtmgrpc.GrpcClientLog))
	dtmcli.FatalIfError(err)
	DtmClient = dtmgrpc.NewDtmClient(conn)
	dtmcli.Logf("dtm client inited")

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", BusiGrpcPort))
	dtmcli.FatalIfError(err)
	s := grpc.NewServer(grpc.UnaryInterceptor(dtmgrpc.GrpcServerLog))
	s.RegisterService(&busi.Busi_ServiceDesc, &busiServer{})
	go func() {
		dtmcli.Logf("busi grpc listening at %v", lis.Addr())
		err := s.Serve(lis)
		dtmcli.FatalIfError(err)
	}()
	time.Sleep(100 * time.Millisecond)
}

func (s *busiServer) TransInRevert(ctx context.Context, in *dtmgrpc.BusiRequest) (*dtmgrpc.BusiReply, error) {
	req := TransReq{}
	dtmcli.MustUnmarshal(in.BusiData, &req)
	return &dtmgrpc.BusiReply{}, handleGrpcBusiness(in, "", "", dtmcli.GetFuncName())
}

func (s *busiServer) TransOutRevert(ctx context.Context, in *dtmgrpc.BusiRequest) (*dtmgrpc.BusiReply, error) {
	req := TransReq{}
	dtmcli.MustUnmarshal(in.BusiData, &req)
	return &dtmgrpc.BusiReply{}, handleGrpcBusiness(in, "", "", dtmcli.GetFuncName())
}

func (s *busiServer) TransInConfirm(ctx context.Context, in *dtmgrpc.BusiRequest) (*dtmgrpc.BusiReply, error) {
	req := TransReq{}
	dtmcli.MustUnmarshal(in.BusiData, &req)
	return &dtmgrpc.BusiReply{}, handleGrpcBusiness(in, "", "", dtmcli.GetFuncName())
}

func (s *busiServer) TransOutConfirm(ctx context.Context, in *dtmgrpc.BusiRequest) (*dtmgrpc.BusiReply, error) {
	req := TransReq{}
	dtmcli.MustUnmarshal(in.BusiData, &req)
	return &dtmgrpc.BusiReply{}, handleGrpcBusiness(in, "", "", dtmcli.GetFuncName())
}

func (s *busiServer) TransInTcc(ctx context.Context, in *dtmgrpc.BusiRequest) (*dtmgrpc.BusiReply, error) {
	req := TransReq{}
	dtmcli.MustUnmarshal(in.BusiData, &req)
	return &dtmgrpc.BusiReply{BusiData: []byte("reply")}, handleGrpcBusiness(in, "", req.TransInResult, dtmcli.GetFuncName())
}

func (s *busiServer) TransOutTcc(ctx context.Context, in *dtmgrpc.BusiRequest) (*dtmgrpc.BusiReply, error) {
	req := TransReq{}
	dtmcli.MustUnmarshal(in.BusiData, &req)
	return &dtmgrpc.BusiReply{BusiData: []byte("reply")}, handleGrpcBusiness(in, "", req.TransOutResult, dtmcli.GetFuncName())
}

func main() {
	GrpcStartup()
	dtmcli.Logf("tcc simple transaction begin")
	gid := dtmgrpc.MustGenGid(DtmGrpcServer)
	err := dtmgrpc.TccGlobalTransaction(DtmGrpcServer, gid, func(tcc *dtmgrpc.TccGrpc) error {
		data := dtmcli.MustMarshal(&TransReq{Amount: 30})
		_, err := tcc.CallBranch(data, BusiGrpc+"/busi.Busi/TransOutTcc", BusiGrpc+"/busi.Busi/TransOutConfirm", BusiGrpc+"/busi.Busi/TransOutRevert")
		if err != nil {
			return err
		}
		_, err = tcc.CallBranch(data, BusiGrpc+"/busi.Busi/TransInTcc", BusiGrpc+"/busi.Busi/TransInConfirm", BusiGrpc+"/busi.Busi/TransInRevert")
		return err
	})
	dtmcli.FatalIfError(err)
	time.Sleep(20 * time.Second)
}
