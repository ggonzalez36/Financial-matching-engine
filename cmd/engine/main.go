package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/fintech/matching-engine/internal/api"
	"github.com/fintech/matching-engine/internal/domain"
	"github.com/fintech/matching-engine/internal/engine"
	"github.com/fintech/matching-engine/internal/storage/wal"
)

const (
	serverPort = ":8080"
	dataDir    = "./data"
)

func main() {
	printBanner()

	// Ensure data directory for WAL files exists
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		log.Fatalf("failed to create data dir: %v", err)
	}

	// 1. Initialize Engine Dispatcher
	dispatcher := engine.NewEngineDispatcher()

	symbols := []string{"BTC-USDT", "ETH-USDT", "SOL-USDT"}

	for _, sym := range symbols {
		walPath := filepath.Join(dataDir, fmt.Sprintf("%s.wal", sym))

		// 2. Perform Recovery from WAL if exists
		recoveredBook, count, err := engine.RecoverOrderBook(sym, walPath)
		if err != nil {
			log.Printf("[%s] WAL recovery warning: %v", sym, err)
		} else if count > 0 {
			log.Printf("[%s] State recovered successfully: %d records replayed. Active orders: %d",
				sym, count, recoveredBook.OrderCount())
		}

		// 3. Open WAL for live engine mutations (sync on write = false for microsecond throughput, bufio = 128KB)
		walLog, err := wal.Open(walPath, 128*1024, false)
		if err != nil {
			log.Fatalf("[%s] Failed to open WAL: %v", sym, err)
		}

		// 4. Register symbol worker in dispatcher
		dispatcher.RegisterSymbol(sym, 65536, walLog)
		log.Printf("[%s] Matching worker initialized with 64K inbound queue and binary WAL.", sym)
	}

	// 5. Seed initial market liquidity for demonstration
	seedDemoLiquidity(dispatcher)

	// 6. Start API Gateway
	srv := api.NewServer(dispatcher)
	httpServer := &http.Server{
		Addr:    serverPort,
		Handler: srv.Router(),
	}

	go func() {
		log.Printf("Matching Engine API & WebSocket Gateway running at http://localhost%s", serverPort)
		log.Printf("Runtime pprof profiling endpoints available at http://localhost%s/debug/pprof/", serverPort)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	// 7. Graceful Shutdown on SIGINT/SIGTERM
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)
	<-stopChan

	log.Println("\nShutdown signal received. Flashing buffers and stopping workers...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_ = httpServer.Shutdown(ctx)
	dispatcher.StopAll()

	log.Println("Matching Engine shutdown completed cleanly.")
}

func seedDemoLiquidity(dispatcher *engine.EngineDispatcher) {
	worker, err := dispatcher.GetWorker("BTC-USDT")
	if err != nil {
		return
	}

	// Seed resting ask limits: 60,000, 60,010, 60,020 (in cents/micro-units: 6000000)
	asks := []struct {
		id  domain.OrderID
		px  domain.Price
		qty domain.Quantity
	}{
		{id: 1, px: 6000000, qty: 50},
		{id: 2, px: 6001000, qty: 80},
		{id: 3, px: 6002000, qty: 120},
	}

	for _, a := range asks {
		o := domain.AcquireOrder()
		o.ID = a.id
		o.Side = domain.SideSell
		o.Type = domain.OrderTypeLimit
		o.Price = a.px
		o.Quantity = a.qty
		o.LeavesQty = a.qty
		o.TIF = domain.TimeInForceGTC
		o.Timestamp = time.Now().UnixNano()
		_ = worker.Submit(engine.Command{
			Type:  engine.CmdPlaceOrder,
			Order: o,
		})
	}

	// Seed resting bid limits: 59,990, 59,980, 59,970
	bids := []struct {
		id  domain.OrderID
		px  domain.Price
		qty domain.Quantity
	}{
		{id: 4, px: 5999000, qty: 60},
		{id: 5, px: 5998000, qty: 90},
		{id: 6, px: 5997000, qty: 150},
	}

	for _, b := range bids {
		o := domain.AcquireOrder()
		o.ID = b.id
		o.Side = domain.SideBuy
		o.Type = domain.OrderTypeLimit
		o.Price = b.px
		o.Quantity = b.qty
		o.LeavesQty = b.qty
		o.TIF = domain.TimeInForceGTC
		o.Timestamp = time.Now().UnixNano()
		_ = worker.Submit(engine.Command{
			Type:  engine.CmdPlaceOrder,
			Order: o,
		})
	}

	log.Println("[BTC-USDT] Seeded initial L2 market liquidity (Bids: 59,970-59,990 | Asks: 60,000-60,020)")
}

func printBanner() {
	banner := `
========================================================================
   __  ___      __       __    _            ____            _            
  /  |/  /___ _/ /______/ /_  (_)___  ____ _/ __/___  ____ _(_)___  ___  
 / /|_/ / __ '/ __/ ___/ __ \/ / __ \/ __ '/ /_/ __ \/ __ '/ / __ \/ _ \ 
/ /  / / /_/ / /_/ /__/ / / / / / / / /_/ / __/ /_/ / /_/ / / / / /  __/ 
/_/  /_/\__,_/\__/\___/_/ /_/_/_/ /_/\__, /_/  \____/\__, /_/_/ /_/\___/  
                                    /____/          /____/               
High-Performance In-Memory Limit Order Book Matching Engine (Go / HFT)
========================================================================`
	fmt.Println(banner)
}
