package router

import (
	"github.com/gin-gonic/gin"

	"ingest_data/internal/interfaces/http/handler"
)

func RegisterRoutes(r *gin.Engine, orderHandler *handler.OrderHandler) {
	api := r.Group("/api")
	{
		api.POST("/orders", orderHandler.CreateOrder)
	}
}
