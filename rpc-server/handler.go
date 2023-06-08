package main

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/redis/go-redis/v9"
	"github.com/sytan98/Backend-Instant-Messaging-System-Assignment/rpc-server/kitex_gen/rpc"
)

// IMServiceImpl implements the last service interface defined in the IDL.
type IMServiceImpl struct {
	redisClient *redis.Client
}

func (s *IMServiceImpl) Send(ctx context.Context, req *rpc.SendRequest) (*rpc.SendResponse, error) {
	resp := rpc.NewSendResponse()
	serialized, _ := json.Marshal(req.Message)

	err := s.redisClient.ZAdd(ctx, req.Message.Chat, redis.Z{
		Score:  float64(req.Message.SendTime),
		Member: serialized,
	}).Err()
	// if there has been an error setting the value
	// handle the error
	if err != nil {
		resp.Code, resp.Msg = 500, "error"
		return resp, errors.New("redis error")
	} else {
		resp.Code, resp.Msg = 0, "success"
		return resp, nil
	}
}

func (s *IMServiceImpl) Pull(ctx context.Context, req *rpc.PullRequest) (*rpc.PullResponse, error) {
	resp := rpc.NewPullResponse()
	pipe := s.redisClient.TxPipeline()

	countResult := pipe.ZCount(ctx, req.Chat, strconv.FormatInt(req.Cursor, 10), "+inf")
	zrange := redis.ZRangeBy{
		Min:    strconv.FormatInt(req.Cursor, 10),
		Max:    "+inf",
		Offset: 0,
		Count:  int64(req.Limit) + 1,
	}

	zrangeResult := pipe.ZRangeByScore(ctx, req.Chat, &zrange)

	_, err := pipe.Exec(ctx)

	if err != nil {
		resp.Code, resp.Msg = 500, "error"
		return resp, err
	} else {
		var messages []*rpc.Message
		for _, val := range zrangeResult.Val() {
			var message rpc.Message
			json.Unmarshal([]byte(val), &message)
			messages = append(messages, &message)
		}

		resp.Code, resp.Msg = 0, "success"
		var messages_len int
		if len(messages) > int(req.Limit) {
			messages_len = len(messages) - 1
		} else {
			messages_len = len(messages)
		}
		hasMore := int(countResult.Val()) > messages_len
		resp.HasMore = &hasMore
		if hasMore {
			resp.NextCursor = &messages[len(messages)-1].SendTime
			messages = messages[:len(messages)-1]
		}
		if *req.Reverse {
			reverseArrayInPlace(messages)
		}
		resp.Messages = messages
		return resp, nil
	}
}

func reverseArrayInPlace(arr []*rpc.Message) []*rpc.Message {
	for i, j := 0, len(arr)-1; i < j; i, j = i+1, j-1 {
		arr[i], arr[j] = arr[j], arr[i]
	}
	return arr
}
