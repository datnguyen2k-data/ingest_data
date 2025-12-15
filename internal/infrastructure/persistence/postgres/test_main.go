package postgres

import (
	"log"
	"os"
	"testing"

	"github.com/joho/godotenv"
)

func TestMain(m *testing.M) {
	// Load .env from project root
	if err := godotenv.Load("../../../.env"); err != nil {
		log.Println("warning: .env not loaded:", err)
	}

	code := m.Run()
	os.Exit(code)
}