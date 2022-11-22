package decoder

import (
	"context"
	"fmt"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/osmosis-labs/osmosis/v12/app/params"
	"google.golang.org/grpc"
	"log"
	"net"
)

type Decoder struct {
	EncodingConfig params.EncodingConfig
}

func (d *Decoder) ListenAndServe(port string) error {
	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
		return err
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	RegisterCosmosDecoderServer(grpcServer, d)
	return grpcServer.Serve(lis)
}

func (d *Decoder) mustEmbedUnimplementedCosmosDecoderServer() {
	panic("Forward-compatibility: Unknown method!")
}

func (d *Decoder) Decode(ctx context.Context, request *DecodeRequest) (*DecodeResponse, error) {
	cosmosTx, err := d.EncodingConfig.TxConfig.TxDecoder()(request.TxByte)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	jsonpbMarshaller := jsonpb.Marshaler{}

	var msgs []*GeneralCosmosMsg

	for _, msg := range cosmosTx.GetMsgs() {
		msgString, err := jsonpbMarshaller.MarshalToString(msg)
		if err != nil {
			return nil, err
		}
		msgType := sdk.MsgTypeURL(msg)

		msgs = append(msgs, &GeneralCosmosMsg{
			Type:    msgType,
			Message: msgString,
			Signers: getSliceFromAccAddress(msg.GetSigners()),
		})
	}

	return &DecodeResponse{Msgs: msgs}, nil
}

func getSliceFromAccAddress(addrs []sdk.AccAddress) []string {
	var results []string
	var sMap map[string]string
	for _, s := range addrs {
		if _, ok := sMap[s.String()]; ok {
			continue
		}
		sMap[s.String()] = "exists"
		results = append(results, s.String())
	}
	return results
}
