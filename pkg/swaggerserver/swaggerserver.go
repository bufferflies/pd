// Copyright 2020 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build swagger_server
// +build swagger_server

package swaggerserver

import (
	"context"
	"net/http"

<<<<<<< HEAD
=======
	httpSwagger "github.com/swaggo/http-swagger"
	_ "github.com/tikv/pd/docs/swagger"
	"github.com/tikv/pd/pkg/utils/apiutil"
>>>>>>> c07c333b3 (swagger: block swagger url if disbale swagger server  (#6785))
	"github.com/tikv/pd/server"
)

const swaggerPrefix = "/swagger/"

var (
	swaggerServiceGroup = server.ServiceGroup{
		Name:       "swagger",
		Version:    "v1",
		IsCore:     false,
		PathPrefix: swaggerPrefix,
	}
)

// Enabled return true if swagger server is disabled.
func Enabled() bool {
	return true
}

// NewHandler creates a HTTP handler for Swagger.
func NewHandler(context.Context, *server.Server) (http.Handler, server.ServiceGroup, error) {
	swaggerHandler := http.NewServeMux()
	swaggerHandler.Handle(swaggerPrefix, httpSwagger.Handler())
	return swaggerHandler, swaggerServiceGroup, nil
}
