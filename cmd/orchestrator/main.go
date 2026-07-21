package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net"

	pb "github.com/vatsalchaudhary/loadforge/proto"
	"github.com/vatsalchaudhary/loadforge/pkg/testplan"
	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedWorkerControlServer
	testPlanJSON []byte
}

func main() {
	log.Println("Starting LoadForge Orchestrator skeleton...")

	// Create a hardcoded dummy test plan for Milestone 1
	dummyPlan := testplan.TestPlan{
		Name:    "Skeleton Test Plan",
		Version: "1.0",
		Target: testplan.Target{
			BaseURL:       "https://httpbin.org",
			TLSSkipVerify: true,
			Timeout:       "10s",
		},
		LoadProfile: testplan.LoadProfile{
			Type:           "constant",
			InitialWorkers: 1,
			MaxWorkers:     1,
			HoldDuration:   "1m",
		},
		Scenarios: []testplan.Scenario{
			{
				Name:   "Get Status",
				Weight: 1.0,
				Steps: []testplan.Step{
					{
						Name:      "GET /status/200",
						Method:    "GET",
						Path:      "/status/200",
						ThinkTime: "1s",
					},
				},
			},
		},
		Workers: testplan.WorkerSpec{
			Resources: testplan.ResourceSpec{
				CPU:    "200m",
				Memory: "256Mi",
			},
			VirtualUsersPerWorker: 5,
		},
	}

	planBytes, err := json.Marshal(dummyPlan)
	if err != nil {
		log.Fatalf("Failed to marshal dummy test plan: %v", err)
	}

	log.Printf("Loaded default hardcoded test plan: %s", string(planBytes))

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen on port 50051: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterWorkerControlServer(s, &server{testPlanJSON: planBytes})

	log.Println("Orchestrator gRPC server listening on :50051")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

func (s *server) Register(ctx context.Context, req *pb.WorkerInfo) (*pb.TestPlanResponse, error) {
	log.Printf("[Register] Worker registered: ID=%s, RunID=%s, IP=%s, Node=%s",
		req.WorkerId, req.RunId, req.PodIp, req.NodeName)

	return &pb.TestPlanResponse{
		RunId:        req.RunId,
		PlanJson:     s.testPlanJSON,
		WorkerIndex:  0,
		TotalWorkers: 1,
	}, nil
}

func (s *server) Heartbeat(stream pb.WorkerControl_HeartbeatServer) error {
	log.Println("[Heartbeat] Stream opened by worker")
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			log.Println("[Heartbeat] Stream closed by worker (EOF)")
			return nil
		}
		if err != nil {
			log.Printf("[Heartbeat] Error receiving heartbeat: %v", err)
			return err
		}

		log.Printf("[Heartbeat] Received from Worker=%s, Run=%s (ActiveGoroutines=%d, CPU=%.2f%%, Memory=%d bytes, RPS=%.2f, TotalReqs=%d, TotalErrs=%d)",
			req.WorkerId, req.RunId,
			req.Stats.ActiveGoroutines, req.Stats.CpuPercent, req.Stats.MemoryBytes,
			req.Stats.RequestsPerSec, req.Stats.TotalRequests, req.Stats.TotalErrors)

		// Send CONTINUE signal back
		resp := &pb.HeartbeatResponse{
			Signal: "CONTINUE",
		}
		if err := stream.Send(resp); err != nil {
			log.Printf("[Heartbeat] Error sending heartbeat response: %v", err)
			return err
		}
	}
}

func (s *server) Stop(ctx context.Context, req *pb.StopRequest) (*pb.StopResponse, error) {
	log.Printf("[Stop] Received stop request for Worker=%s, RunID=%s", req.WorkerId, req.RunId)
	return &pb.StopResponse{
		Acknowledged:    true,
		DrainedRequests: 0,
	}, nil
}
