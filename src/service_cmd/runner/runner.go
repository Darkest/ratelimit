package runner

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"io"
	"math/rand"
	"net/http"
	"time"

	stats "github.com/lyft/gostats"

	"github.com/coocood/freecache"

	pb_legacy "github.com/envoyproxy/go-control-plane/envoy/service/ratelimit/v2"
	pb "github.com/envoyproxy/go-control-plane/envoy/service/ratelimit/v3"

	"github.com/envoyproxy/ratelimit/src/config"
	"github.com/envoyproxy/ratelimit/src/limiter"
	"github.com/envoyproxy/ratelimit/src/redis"
	"github.com/envoyproxy/ratelimit/src/server"
	ratelimit "github.com/envoyproxy/ratelimit/src/service"
	"github.com/envoyproxy/ratelimit/src/settings"
	logger "github.com/sirupsen/logrus"
)

type Runner struct {
	statsStore stats.Store
}

// Stats for an individual rate limit config entry.
type RequestsStats struct {
	LastDuration stats.Gauge
}

func newRateLimitStats(statsScope stats.Scope) RequestsStats {
	ret := RequestsStats{}
	ret.LastDuration = statsScope.NewGauge("ratelimiter_last_duration_ns")
	return ret
}

func NewRunner() Runner {
	return Runner{stats.NewDefaultStore()}
}

func (runner *Runner) GetStatsStore() stats.Store {
	return runner.statsStore
}

//Interceptor for request metrics
func unaryServerInterceptor(rqStats RequestsStats) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		now := time.Now()
		md, ok := metadata.FromIncomingContext(ctx)
		fmt.Printf("MD exists: %b, MD: %s\n", ok, md)
		resp, err := handler(ctx, req)
		time.Sleep(time.Millisecond * 10)
		rqStats.LastDuration.Set(uint64(time.Since(now).Nanoseconds()))
		return resp, err
	}
}

func (runner *Runner) Run() {
	s := settings.NewSettings()

	logLevel, err := logger.ParseLevel(s.LogLevel)
	if err != nil {
		logger.Fatalf("Could not parse log level. %v\n", err)
	} else {
		logger.SetLevel(logLevel)
	}
	var localCache *freecache.Cache
	if s.LocalCacheSizeInBytes != 0 {
		localCache = freecache.NewCache(s.LocalCacheSizeInBytes)
	}

	rls := newRateLimitStats(runner.statsStore)
	srv := server.NewServer("ratelimit", runner.statsStore, localCache, settings.GrpcUnaryInterceptor(unaryServerInterceptor(rls)))

	service := ratelimit.NewService(
		srv.Runtime(),
		redis.NewRateLimiterCacheImplFromSettings(
			s,
			localCache,
			srv,
			limiter.NewTimeSourceImpl(),
			rand.New(limiter.NewLockedSource(time.Now().Unix())),
			s.ExpirationJitterMaxSeconds),
		config.NewRateLimitConfigLoaderImpl(),
		srv.Scope().Scope("service"),
		s.RuntimeWatchRoot,
	)

	srv.AddDebugHttpEndpoint(
		"/rlconfig",
		"print out the currently loaded configuration for debugging",
		func(writer http.ResponseWriter, request *http.Request) {
			io.WriteString(writer, service.GetCurrentConfig().Dump())
		})

	srv.AddJsonHandler(service)

	// Ratelimit is compatible with two proto definitions
	// 1. data-plane-api v3 rls.proto: https://github.com/envoyproxy/data-plane-api/blob/master/envoy/service/ratelimit/v3/rls.proto
	pb.RegisterRateLimitServiceServer(srv.GrpcServer(), service)
	// 1. data-plane-api v2 rls.proto: https://github.com/envoyproxy/data-plane-api/blob/master/envoy/service/ratelimit/v2/rls.proto
	pb_legacy.RegisterRateLimitServiceServer(srv.GrpcServer(), service.GetLegacyService())
	// (1) is the current definition, and (2) is the legacy definition.

	srv.Start()
}
