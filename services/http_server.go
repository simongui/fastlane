package services

import (
	"fmt"
	"log"

	"github.com/simongui/fastlane/storage"
	"github.com/valyala/fasthttp"
)

// HTTPServer Represents an instance of the HTTP protocol server.
type HTTPServer struct {
	started bool
	store   storage.Store
}

// NewHTTPServer Returns a new HTTPServer instance.
func NewHTTPServer(store storage.Store) *HTTPServer {
	server := &HTTPServer{store: store}
	return server
}

// IsStarted Returns whether the HTTP protocol server is started.
func (server *HTTPServer) IsStarted() bool {
	return server.started
}

// ListenAndServe Starts the HTTP protocol server.
func (server *HTTPServer) ListenAndServe(address string) {
	h := requestHandler
	// h = fasthttp.CompressHandler(h)

	server.started = true
	if err := fasthttp.ListenAndServe(address, h); err != nil {
		server.started = false
		log.Fatalf("Error in ListenAndServe: %s", err)
	}
}

func requestHandler(ctx *fasthttp.RequestCtx) {
	fmt.Fprintf(ctx, "Hello, world!\n\n")

	// fmt.Fprintf(ctx, "Request method is %q\n", ctx.Method())
	// fmt.Fprintf(ctx, "RequestURI is %q\n", ctx.RequestURI())
	// fmt.Fprintf(ctx, "Requested path is %q\n", ctx.Path())
	// fmt.Fprintf(ctx, "Host is %q\n", ctx.Host())
	// fmt.Fprintf(ctx, "Query string is %q\n", ctx.QueryArgs())
	// fmt.Fprintf(ctx, "User-Agent is %q\n", ctx.UserAgent())
	// fmt.Fprintf(ctx, "Connection has been established at %s\n", ctx.ConnTime())
	// fmt.Fprintf(ctx, "Request has been started at %s\n", ctx.Time())
	// fmt.Fprintf(ctx, "Serial request number for the current connection is %d\n", ctx.ConnRequestNum())
	// fmt.Fprintf(ctx, "Your ip is %q\n\n", ctx.RemoteIP())
	//
	// fmt.Fprintf(ctx, "Raw request is:\n---CUT---\n%s\n---CUT---", &ctx.Request)

	ctx.SetContentType("text/plain; charset=utf8")
}
