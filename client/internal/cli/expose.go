package cli

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/horizon/client/internal/tunnel"
)

var serverWS string

var exposeCmd = &cobra.Command{
	Use:   "expose <port>",
	Short: "Expose a local TCP port via the Horizon tunnel",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		port, err := strconv.Atoi(args[0])
		if err != nil || port <= 0 || port > 65535 {
			return fmt.Errorf("invalid port %q", args[0])
		}
		u, err := parseServerURL(serverWS)
		if err != nil {
			return err
		}
		log.Printf("connecting to %s …", redactWS(u))
		for {
			err := tunnel.Run(cmd.Context(), u.String(), port)
			if err != nil {
				log.Printf("tunnel error: %v — reconnecting in 2s", err)
				select {
				case <-cmd.Context().Done():
					return cmd.Context().Err()
				case <-time.After(2 * time.Second):
				}
				continue
			}
			return nil
		}
	},
}

func init() {
	exposeCmd.Flags().StringVar(&serverWS, "server", defaultServer(), "WebSocket URL of tunnel endpoint")
}

func defaultServer() string {
	if v := strings.TrimSpace(os.Getenv("HORIZON_SERVER_WS")); v != "" {
		return strings.TrimSuffix(v, "/")
	}
	return "ws://127.0.0.1:8080/tunnel/connect"
}

func parseServerURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "ws" && u.Scheme != "wss" {
		return nil, fmt.Errorf("server URL must use ws or wss scheme")
	}
	if u.Host == "" {
		return nil, fmt.Errorf("server URL needs a host")
	}
	return u, nil
}

func redactWS(u *url.URL) string {
	cp := *u
	if cp.User != nil {
		cp.User = url.UserPassword("redacted", "")
	}
	return cp.String()
}
