// Command nativeprobe tests the native API of a disposable loopback gateway.
// It writes only a fresh marker in /sandbox and leaves lifecycle cleanup to
// the operator. A timeout response is not proof of remote command shutdown.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"

	dm "github.com/NVIDIA/OpenShell/sdk/go/proto/datamodelv1"
	pb "github.com/NVIDIA/OpenShell/sdk/go/proto/openshellv1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/durationpb"
)

type observation struct {
	Case      string `json:"case"`
	RequestID string `json:"request_id"`
	Output    string `json:"output"`
	Exit      *int32 `json:"exit,omitempty"`
	Error     string `json:"error,omitempty"`
}

func receive(client pb.OpenShellClient, name string, request *pb.ExecSandboxRequest) observation {
	return receiveWithCancellation(client, name, request, false)
}

func receiveWithCancellation(client pb.OpenShellClient, name string, request *pb.ExecSandboxRequest, cancelWhenStarted bool) observation {
	result := observation{Case: name, RequestID: request.RequestId}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	stream, err := client.ExecSandbox(ctx, request)
	if err == nil {
		for {
			event, readErr := stream.Recv()
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				err = readErr
				break
			}
			if out := event.GetStdout(); out != nil {
				result.Output += string(out.Data)
			}
			if out := event.GetStderr(); out != nil {
				result.Output += string(out.Data)
			}
			if exit := event.GetExit(); exit != nil {
				value := exit.ExitCode
				result.Exit = &value
			}
			if cancelWhenStarted && strings.Contains(result.Output, "cancel-command-started\n") {
				cancel()
			}
		}
	}
	if err != nil {
		result.Error = err.Error()
	}
	return result
}

func main() {
	endpoint := flag.String("endpoint", "", "disposable gateway loopback address and port")
	sandbox := flag.String("sandbox", "", "existing disposable sandbox name")
	workspace := flag.String("workspace", "default", "gateway workspace")
	flag.Parse()
	host, _, err := net.SplitHostPort(*endpoint)
	if err != nil || !net.ParseIP(host).IsLoopback() || *sandbox == "" {
		fmt.Fprintln(os.Stderr, "Specify a numeric loopback endpoint and disposable sandbox name")
		os.Exit(2)
	}
	conn, err := grpc.NewClient(*endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer conn.Close()
	client := pb.NewOpenShellClient(conn)
	request := func(script string) *pb.ExecSandboxRequest {
		return &pb.ExecSandboxRequest{Sandbox: *sandbox, RequestId: uuid.NewString(), NoLoginShell: true,
			WorkspaceScope: &dm.WorkspaceSelector{Selection: &dm.WorkspaceSelector_Workspace{Workspace: *workspace}},
			Command:        []string{"/bin/sh", "-c", script}}
	}
	marker := "/sandbox/kingdom-native-probe-" + uuid.NewString()
	first := request("printf 'once\\n' >> " + marker)
	results := []observation{receive(client, "initial", first), receive(client, "duplicate", first)}
	results = append(results, receive(client, "marker", request("cat "+marker)))
	timed := request("trap 'exit 0' TERM; printf 'timeout-command-started\\n'; while :; do sleep 1; done")
	timed.ExecutionTimeout = durationpb.New(2 * time.Second)
	results = append(results, receive(client, "native-execution-timeout", timed))
	cancelled := request("trap '' TERM; printf 'cancel-command-started\\n'; while :; do sleep 1; done")
	results = append(results, receiveWithCancellation(client, "native-client-cancellation", cancelled, true))
	_ = json.NewEncoder(os.Stdout).Encode(results)
	if results[0].Error != "" || results[0].Exit == nil || *results[0].Exit != 0 || results[1].Error == "" ||
		results[2].Error != "" || strings.TrimSpace(results[2].Output) != "once" {
		os.Exit(1)
	}
	if results[3].Exit != nil && *results[3].Exit == 0 {
		os.Exit(1)
	}
	if results[4].Error == "" || !strings.Contains(results[4].Output, "cancel-command-started\n") || results[4].Exit != nil {
		os.Exit(1)
	}
}
