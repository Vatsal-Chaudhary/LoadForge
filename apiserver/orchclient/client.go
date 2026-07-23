package orchclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	pb "github.com/vatsalchaudhary/loadforge/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	conn   *grpc.ClientConn
	client pb.OperatorControlClient
}

func Dial(ctx context.Context, address string) (*Client, error) {
	if address == "" {
		return nil, errors.New("ORCHESTRATOR_ADDR is required")
	}
	dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(dialCtx, address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("connect orchestrator: %w", err)
	}
	out := &Client{conn: conn, client: pb.NewOperatorControlClient(conn)}
	if err := out.Ready(dialCtx); err != nil {
		conn.Close()
		return nil, err
	}
	return out, nil
}

func (c *Client) Submit(ctx context.Context, runID string, plan json.RawMessage) (string, error) {
	resp, err := c.client.SubmitRun(ctx, &pb.SubmitRunRequest{RunId: runID, PlanJson: plan})
	if err != nil {
		return "", err
	}
	return resp.Status, nil
}

func (c *Client) Stop(ctx context.Context, runID string) (string, error) {
	resp, err := c.client.StopRun(ctx, &pb.StopRunRequest{RunId: runID})
	if err != nil {
		return "", err
	}
	return resp.Status, nil
}

func (c *Client) Ready(ctx context.Context) error {
	resp, err := c.client.Health(ctx, &pb.HealthRequest{})
	if err != nil {
		return err
	}
	if !resp.Serving {
		return errors.New("orchestrator is not serving")
	}
	return nil
}

func (c *Client) Close() error { return c.conn.Close() }
