package server
import (
 "io"
 "context"
 "errors"
 api "github.com/travisjeffery/proglog/api/v1"
 "google.golang.org/grpc"
 "google.golang.org/grpc/status"
 "google.golang.org/grpc/codes"
)
type Config struct {
 CommitLog CommitLog
}
var _ api.LogServer = (*grpcServer)(nil)
func NewGRPCServer(config *Config, grpcOpts ...grpc.ServerOption) (*grpc.Server, error) {
 gsrv := grpc.NewServer(grpcOpts...)
 srv, err := newgrpcServer(config)
 if err != nil {
  return nil, err
 }
 api.RegisterLogServer(gsrv, srv)
 return gsrv, nil
}
type grpcServer struct {
 api.UnimplementedLogServer
 *Config
}
type CommitLog interface {
 Append(*api.Record) (uint64, error)
 Read(uint64) (*api.Record, error)
}
func newgrpcServer(config *Config) (srv *grpcServer, err error) {
 if config == nil {
  panic("config is nil")
 }
 if config.CommitLog == nil {
  panic("CommitLog is nil")
 }
 srv = &grpcServer{
 Config: config,
 }
 return srv, nil
}
func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (
 *api.ProduceResponse, error) {
  if s.CommitLog == nil{
   panic("CommitLog is nil")
  }
  offset, err := s.CommitLog.Append(req.Record)
  if err != nil {
   return nil, status.Error(codes.Internal, err.Error())
  }
  return &api.ProduceResponse{Offset: offset}, nil
}
func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (
 *api.ConsumeResponse, error) {
  record, err := s.CommitLog.Read(req.Offset)
  if err != nil {
   var outOfRange api.ErrOffsetOutOfRange
   if errors.As(err, &outOfRange) {
    return nil, status.Error(codes.OutOfRange, err.Error())
   }
   return nil, status.Error(codes.Internal, err.Error())
  }
  return &api.ConsumeResponse{Record: record}, nil
}
func (s *grpcServer) ProduceStream(
 stream api.Log_ProduceStreamServer,
 ) error {
  for {
   req, err := stream.Recv()
   if err == io.EOF {
    return nil
   }
   if err != nil {
    return err
   }
   res, err := s.Produce(stream.Context(), req)
   if err != nil {
    return err
   }
   if res == nil {
    return status.Error(codes.Internal, "nil response")
   }
   if err := stream.Send(res); err != nil {
    return err
   }
  }
}
func (s *grpcServer) ConsumeStream(
 req *api.ConsumeRequest,
 stream api.Log_ConsumeStreamServer,
 ) error {
  for {
     res, err := s.Consume(stream.Context(), req)
     if err != nil {
      return err
     }
     if res == nil {
      return status.Error(codes.Internal, "nil response")
     }
     if err := stream.Send(res); err != nil { return err }
   req.Offset++
  }
}