package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"

	app "ingest_data/internal/application/order"
)

type OrderHandler struct {
	svc *app.Service
}

func NewOrderHandler(svc *app.Service) *OrderHandler {
	return &OrderHandler{svc: svc}
}

func (h *OrderHandler) CreateOrder(c *gin.Context) {
	var cmd app.CreateOrderCommand
	if err := c.ShouldBindJSON(&cmd); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	order, err := h.svc.SubmitOrder(c.Request.Context(), cmd)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{
		"id":      order.ID,
		"status":  order.Status,
		"message": "order accepted and queued",
	})
}
