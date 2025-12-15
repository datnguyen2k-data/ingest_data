package gin

import (
	"fmt"

	ginlib "github.com/gin-gonic/gin"

	"ingest_data/internal/config"
)

type Server struct {
	engine *ginlib.Engine
	addr   string
}

func NewEngine() *ginlib.Engine {
	r := ginlib.New()
	r.Use(ginlib.Recovery())
	return r
}

func NewServer(cfg config.ServerConfig, engine *ginlib.Engine) *Server {
	return &Server{
		engine: engine,
		addr:   cfg.Address(),
	}
}

func (s *Server) Run() error {
	if s.engine == nil {
		return fmt.Errorf("gin engine is nil")
	}
	return s.engine.Run(s.addr)
}
