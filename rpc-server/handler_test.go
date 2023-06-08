package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/sytan98/Backend-Instant-Messaging-System-Assignment/rpc-server/kitex_gen/rpc"
)

var redisServer *miniredis.Miniredis

func TestIMServiceImpl_Send(t *testing.T) {
	redisClient := setup()
	defer teardown()

	type args struct {
		ctx context.Context
		req *rpc.SendRequest
	}
	tests := []struct {
		name    string
		args    args
		wantErr error
	}{
		{
			name: "Send message stored in redis 1",
			args: args{
				ctx: context.Background(),
				req: &rpc.SendRequest{
					Message: &rpc.Message{
						Chat:     "test1:test2",
						Text:     "test_content",
						Sender:   "test2",
						SendTime: 0,
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "Send message stored in redis 2",
			args: args{
				ctx: context.Background(),
				req: &rpc.SendRequest{
					Message: &rpc.Message{
						Chat:     "test3:test4",
						Text:     "test_content",
						Sender:   "test3",
						SendTime: 2,
					},
				},
			},
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &IMServiceImpl{redisClient: redisClient}
			got, err := s.Send(tt.args.ctx, tt.args.req)
			assert.True(t, errors.Is(err, tt.wantErr))
			assert.NotNil(t, got)
			expected, _ := json.Marshal(tt.args.req.Message)
			assert.Equal(t, string(expected), s.redisClient.ZRange(tt.args.ctx, tt.args.req.Message.Chat, 0, -1).Val()[0])
		})
	}
}

func TestIMServiceImpl_Send_RedisError(t *testing.T) {
	redisClient := setup()
	defer teardown()

	type args struct {
		ctx context.Context
		req *rpc.SendRequest
	}
	tests := []struct {
		name    string
		args    args
		wantErr error
	}{
		{
			name: "Send message stored in redis 1",
			args: args{
				ctx: context.Background(),
				req: &rpc.SendRequest{
					Message: &rpc.Message{
						Chat:     "test1:test2",
						Text:     "test_content",
						Sender:   "test2",
						SendTime: 0,
					},
				},
			},
			wantErr: errors.New("redis error"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &IMServiceImpl{redisClient: redisClient}
			redisServer.SetError("redis error")
			_, err := s.Send(tt.args.ctx, tt.args.req)
			assert.EqualError(t, err, tt.wantErr.Error())
		})
	}
}

func TestIMServiceImpl_Pull(t *testing.T) {
	redisClient := setup()
	defer teardown()
	msgs := []*rpc.Message{
		{
			Chat:     "test1:test2",
			Text:     "test_content",
			Sender:   "test2",
			SendTime: 1,
		},
		{
			Chat:     "test3:test4",
			Text:     "test_content_2",
			Sender:   "test3",
			SendTime: 2,
		},
		{
			Chat:     "test5:test6",
			Text:     "test_content_3",
			Sender:   "test6",
			SendTime: 4,
		},
	}

	type args struct {
		ctx context.Context
		req *rpc.PullRequest
	}
	tests := []struct {
		name               string
		args               args
		wantErr            error
		expectedMsgs       []*rpc.Message
		expectedHasMore    bool
		expectedNextCursor int64
	}{
		{
			name: "Pull all message from redis",
			args: args{
				ctx: context.Background(),
				req: &rpc.PullRequest{
					Chat:    "test1:test2",
					Cursor:  0,
					Limit:   10,
					Reverse: func() *bool { b := false; return &b }(),
				},
			},
			wantErr:            nil,
			expectedMsgs:       msgs,
			expectedHasMore:    false,
			expectedNextCursor: 0,
		},
		{
			name: "Pull message from redis with limit",
			args: args{
				ctx: context.Background(),
				req: &rpc.PullRequest{
					Chat:    "test1:test2",
					Cursor:  0,
					Limit:   2,
					Reverse: func() *bool { b := false; return &b }(),
				},
			},
			wantErr:            nil,
			expectedMsgs:       msgs[:2],
			expectedHasMore:    true,
			expectedNextCursor: 4,
		},
		{
			name: "Pull message from redis with reverse",
			args: args{
				ctx: context.Background(),
				req: &rpc.PullRequest{
					Chat:    "test1:test2",
					Cursor:  0,
					Limit:   2,
					Reverse: func() *bool { b := true; return &b }(),
				},
			},
			wantErr:            nil,
			expectedMsgs:       reverseArray(msgs[:2]),
			expectedHasMore:    true,
			expectedNextCursor: 4,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, msg := range msgs {
				serialized, _ := json.Marshal(msg)
				redisClient.ZAdd(tt.args.ctx, tt.args.req.Chat, redis.Z{
					Score:  float64(msg.SendTime),
					Member: serialized,
				})
			}
			s := &IMServiceImpl{redisClient: redisClient}
			got, err := s.Pull(tt.args.ctx, tt.args.req)
			assert.True(t, errors.Is(err, tt.wantErr))
			assert.NotNil(t, got)
			assert.Equal(t, tt.expectedMsgs, got.Messages)
			assert.Equal(t, tt.expectedHasMore, *got.HasMore)
			if tt.expectedNextCursor != 0 {
				assert.Equal(t, tt.expectedNextCursor, *got.NextCursor)
			}
		})
	}
}

func TestIMServiceImpl_Pull_RedisError(t *testing.T) {
	redisClient := setup()
	defer teardown()

	type args struct {
		ctx context.Context
		req *rpc.PullRequest
	}
	tests := []struct {
		name    string
		args    args
		wantErr error
	}{
		{
			name: "Pull message from redis 1",
			args: args{
				ctx: context.Background(),
				req: &rpc.PullRequest{
					Chat:    "test1:test2",
					Cursor:  0,
					Limit:   10,
					Reverse: func() *bool { b := true; return &b }(),
				},
			},
			wantErr: errors.New("redis error"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &IMServiceImpl{redisClient: redisClient}
			redisServer.SetError("redis error")
			_, err := s.Pull(tt.args.ctx, tt.args.req)
			assert.EqualError(t, err, tt.wantErr.Error())
		})
	}
}
func mockRedis() *miniredis.Miniredis {
	s, err := miniredis.Run()

	if err != nil {
		panic(err)
	}

	return s
}

func setup() *redis.Client {
	redisServer = mockRedis()
	return redis.NewClient(&redis.Options{
		Addr: redisServer.Addr(),
	})
}

func teardown() {
	redisServer.Close()
}

func reverseArray(arr []*rpc.Message) []*rpc.Message {
	var result []*rpc.Message
	for i := len(arr) - 1; i >= 0; i-- {
		result = append(result, arr[i])
	}
	return result
}
