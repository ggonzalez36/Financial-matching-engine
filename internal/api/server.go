package api

import (
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fintech/matching-engine/internal/domain"
	"github.com/fintech/matching-engine/internal/engine"
	"github.com/fintech/matching-engine/pkg/protocol"
)

// Server coordinates HTTP REST, WebSocket/SSE streaming, and runtime profiling.
type Server struct {
	dispatcher *engine.EngineDispatcher
	nextID     uint64
	clientsMu  sync.RWMutex
	wsClients  map[net.Conn]bool
	sseClients map[chan string]bool
}

// NewServer initializes the API Gateway.
func NewServer(dispatcher *engine.EngineDispatcher) *Server {
	s := &Server{
		dispatcher: dispatcher,
		nextID:     1000,
		wsClients:  make(map[net.Conn]bool),
		sseClients: make(map[chan string]bool),
	}
	return s
}

// NextOrderID generates a monotonically increasing unique OrderID using lock-free atomics.
func (s *Server) NextOrderID() domain.OrderID {
	return domain.OrderID(atomic.AddUint64(&s.nextID, 1))
}

// Router configures and returns the HTTP handler multiplexer.
func (s *Server) Router() http.Handler {
	mux := http.NewServeMux()

	// REST Endpoints
	mux.HandleFunc("POST /api/v1/order", s.handlePlaceOrder)
	mux.HandleFunc("POST /api/v1/cancel", s.handleCancelOrder)
	mux.HandleFunc("GET /api/v1/depth", s.handleGetDepth)

	// Real-Time Streaming Endpoints
	mux.HandleFunc("GET /ws/stream", s.handleWebSocket)
	mux.HandleFunc("GET /events", s.handleSSE)

	// Runtime Profiling (pprof)
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	return mux
}

// handlePlaceOrder translates an incoming REST request into an engine Command.
func (s *Server) handlePlaceOrder(w http.ResponseWriter, r *http.Request) {
	var dto protocol.PlaceOrderDTO
	if err := json.NewDecoder(r.Body).Decode(&dto); err != nil {
		http.Error(w, `{"error":"invalid json body"}`, http.StatusBadRequest)
		return
	}

	worker, err := s.dispatcher.GetWorker(dto.Symbol)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusNotFound)
		return
	}

	// Parse Side
	var side domain.Side
	switch strings.ToUpper(dto.Side) {
	case "BUY":
		side = domain.SideBuy
	case "SELL":
		side = domain.SideSell
	default:
		http.Error(w, `{"error":"side must be BUY or SELL"}`, http.StatusBadRequest)
		return
	}

	// Parse OrderType
	var oType domain.OrderType
	switch strings.ToUpper(dto.Type) {
	case "LIMIT":
		oType = domain.OrderTypeLimit
	case "MARKET":
		oType = domain.OrderTypeMarket
	default:
		http.Error(w, `{"error":"type must be LIMIT or MARKET"}`, http.StatusBadRequest)
		return
	}

	// Parse TimeInForce
	tif := domain.TimeInForceGTC
	switch strings.ToUpper(dto.TimeInForce) {
	case "IOC":
		tif = domain.TimeInForceIOC
	case "FOK":
		tif = domain.TimeInForceFOK
	}

	// Allocate pooled order struct
	order := domain.AcquireOrder()
	order.ID = s.NextOrderID()
	order.Side = side
	order.Type = oType
	order.Price = domain.Price(dto.Price)
	order.Quantity = domain.Quantity(dto.Quantity)
	order.LeavesQty = order.Quantity
	order.TIF = tif
	order.Timestamp = time.Now().UnixNano()
	order.Status = domain.StatusNew

	respChan := make(chan any, 1)
	cmd := engine.Command{
		Type:     engine.CmdPlaceOrder,
		Order:    order,
		RespChan: respChan,
	}

	if err := worker.Submit(cmd); err != nil {
		domain.ReleaseOrder(order)
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusServiceUnavailable)
		return
	}

	// Await deterministic execution from the symbol actor
	result := <-respChan
	rep := result.(*domain.ExecutionReport)

	respDTO := protocol.OrderResponseDTO{
		OrderID:      uint64(rep.SubjectOrder.ID),
		Symbol:       dto.Symbol,
		Status:       rep.Status.String(),
		LeavesQty:    uint64(rep.SubjectOrder.LeavesQty),
		FilledQty:    uint64(rep.SubjectOrder.FilledQty()),
		TradesCount:  len(rep.Trades),
		RejectReason: rep.RejectReason,
		Timestamp:    rep.Timestamp,
	}

	if len(rep.Trades) > 0 {
		respDTO.Trades = make([]protocol.TradeDTO, len(rep.Trades))
		for i, t := range rep.Trades {
			respDTO.Trades[i] = protocol.FromDomainTrade(t)
		}
	}

	// Broadcast trades to connected streaming subscribers
	if len(rep.Trades) > 0 {
		go s.BroadcastTrades(dto.Symbol, rep.Trades)
	}

	domain.ReleaseExecutionReport(rep)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(respDTO)
}

// handleCancelOrder processes cancellation by OrderID.
func (s *Server) handleCancelOrder(w http.ResponseWriter, r *http.Request) {
	var dto protocol.CancelOrderDTO
	if err := json.NewDecoder(r.Body).Decode(&dto); err != nil {
		http.Error(w, `{"error":"invalid json body"}`, http.StatusBadRequest)
		return
	}

	worker, err := s.dispatcher.GetWorker(dto.Symbol)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusNotFound)
		return
	}

	respChan := make(chan any, 1)
	cmd := engine.Command{
		Type:     engine.CmdCancelOrder,
		CancelID: domain.OrderID(dto.OrderID),
		RespChan: respChan,
	}

	if err := worker.Submit(cmd); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusServiceUnavailable)
		return
	}

	result := <-respChan
	rep := result.(*domain.ExecutionReport)

	respDTO := protocol.OrderResponseDTO{
		OrderID:      dto.OrderID,
		Symbol:       dto.Symbol,
		Status:       rep.Status.String(),
		RejectReason: rep.RejectReason,
		Timestamp:    rep.Timestamp,
	}
	domain.ReleaseExecutionReport(rep)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(respDTO)
}

// handleGetDepth queries the L2 market depth from the matching engine.
func (s *Server) handleGetDepth(w http.ResponseWriter, r *http.Request) {
	symbol := r.URL.Query().Get("symbol")
	limitStr := r.URL.Query().Get("limit")

	limit := 10
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}

	worker, err := s.dispatcher.GetWorker(symbol)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusNotFound)
		return
	}

	respChan := make(chan any, 1)
	cmd := engine.Command{
		Type:     engine.CmdGetDepth,
		Depth:    limit,
		RespChan: respChan,
	}

	if err := worker.Submit(cmd); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusServiceUnavailable)
		return
	}

	depth := (<-respChan).(domain.MarketDepth)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(depth)
}

// handleWebSocket implements an RFC-6455 compliant WebSocket handshake and stream.
func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		http.Error(w, "Expected websocket upgrade", http.StatusBadRequest)
		return
	}

	clientKey := r.Header.Get("Sec-WebSocket-Key")
	if clientKey == "" {
		http.Error(w, "Missing Sec-WebSocket-Key", http.StatusBadRequest)
		return
	}

	// Calculate WebSocket Accept Key
	h := sha1.New()
	h.Write([]byte(clientKey + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	acceptKey := base64.StdEncoding.EncodeToString(h.Sum(nil))

	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "WebSocket hijack not supported", http.StatusInternalServerError)
		return
	}

	conn, bufrw, err := hj.Hijack()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Send HTTP 101 Switching Protocols
	response := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + acceptKey + "\r\n\r\n"

	if _, err := bufrw.WriteString(response); err != nil {
		_ = conn.Close()
		return
	}
	_ = bufrw.Flush()

	s.clientsMu.Lock()
	s.wsClients[conn] = true
	s.clientsMu.Unlock()

	// Keep-alive read pump
	go func() {
		defer func() {
			s.clientsMu.Lock()
			delete(s.wsClients, conn)
			s.clientsMu.Unlock()
			_ = conn.Close()
		}()
		buf := make([]byte, 1024)
		for {
			if _, err := conn.Read(buf); err != nil {
				return
			}
		}
	}()
}

// handleSSE provides a zero-dependency Server-Sent Events stream.
func (s *Server) handleSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch := make(chan string, 128)
	s.clientsMu.Lock()
	s.sseClients[ch] = true
	s.clientsMu.Unlock()

	defer func() {
		s.clientsMu.Lock()
		delete(s.sseClients, ch)
		s.clientsMu.Unlock()
	}()

	notify := r.Context().Done()
	for {
		select {
		case <-notify:
			return
		case msg := <-ch:
			_, _ = fmt.Fprintf(w, "data: %s\n\n", msg)
			flusher.Flush()
		}
	}
}

// BroadcastTrades publishes executed trades to all connected WebSocket and SSE subscribers.
func (s *Server) BroadcastTrades(symbol string, trades []domain.Trade) {
	payload, err := json.Marshal(map[string]any{
		"event":  "trades_executed",
		"symbol": symbol,
		"trades": trades,
		"ts":     time.Now().UnixNano(),
	})
	if err != nil {
		return
	}

	// 1. Send to SSE
	s.clientsMu.RLock()
	for ch := range s.sseClients {
		select {
		case ch <- string(payload):
		default:
		}
	}

	// 2. Send to WebSockets using RFC-6455 binary frame (0x81 = text frame)
	frame := makeWSFrame(payload)
	for conn := range s.wsClients {
		_ = conn.SetWriteDeadline(time.Now().Add(50 * time.Millisecond))
		_, _ = conn.Write(frame)
	}
	s.clientsMu.RUnlock()
}

func makeWSFrame(payload []byte) []byte {
	length := len(payload)
	var header []byte

	if length <= 125 {
		header = []byte{0x81, byte(length)}
	} else if length <= 65535 {
		header = []byte{0x81, 126, byte(length >> 8), byte(length & 0xFF)}
	} else {
		header = []byte{0x81, 127,
			0, 0, 0, 0,
			byte(length >> 24), byte((length >> 16) & 0xFF),
			byte((length >> 8) & 0xFF), byte(length & 0xFF),
		}
	}
	return append(header, payload...)
}
