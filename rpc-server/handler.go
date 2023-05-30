package main

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/redis/go-redis/v9"
	"github.com/sytan98/Backend-Instant-Messaging-System-Assignment/rpc-server/kitex_gen/rpc"
)

// IMServiceImpl implements the last service interface defined in the IDL.
type IMServiceImpl struct{}

func newRedisClient() *redis.Client {
	redisClient := redis.NewClient(&redis.Options{
		Addr:     "redis:6379",
		Password: "", // no password set
		DB:       0,
	})
	return redisClient
}

func (s *IMServiceImpl) Send(ctx context.Context, req *rpc.SendRequest) (*rpc.SendResponse, error) {
	resp := rpc.NewSendResponse()
	redisClient := newRedisClient()
	serialized, _ := json.Marshal(req.Message)

	err := redisClient.ZAdd(ctx, req.Message.Chat, redis.Z{
		Score:  float64(req.Message.SendTime),
		Member: serialized,
	}).Err()
	// if there has been an error setting the value
	// handle the error
	if err != nil {
		resp.Code, resp.Msg = 500, err.Error()
		return resp, err
	} else {
		resp.Code, resp.Msg = 0, "success"
		return resp, nil
	}
}

func (s *IMServiceImpl) Pull(ctx context.Context, req *rpc.PullRequest) (*rpc.PullResponse, error) {
	resp := rpc.NewPullResponse()
	redisClient := newRedisClient()

	totalCount, _ := redisClient.ZCount(ctx, req.Chat, strconv.FormatInt(req.Cursor, 10), "+inf").Result()
	var vals []string
	var err error
	zrange := redis.ZRangeBy{
		Min:    strconv.FormatInt(req.Cursor, 10),
		Max:    "+inf",
		Offset: 0,
		Count:  int64(req.Limit),
	}
	if *req.Reverse {
		vals, err = redisClient.ZRevRangeByScore(ctx, req.Chat, &zrange).Result()
	} else {
		vals, err = redisClient.ZRangeByScore(ctx, req.Chat, &zrange).Result()
	}
	var messages []*rpc.Message
	for _, val := range vals {
		var message rpc.Message
		json.Unmarshal([]byte(val), &message)
		messages = append(messages, &message)
	}

	if err != nil {
		resp.Code, resp.Msg = 500, err.Error()
		return resp, err
	} else {
		resp.Code, resp.Msg = 0, "success"
		resp.Messages = messages
		hasMore := int(totalCount) > len(messages)
		resp.HasMore = &hasMore
		return resp, nil
	}
}
