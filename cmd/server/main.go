package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Grupo-Fixia/md-intake-lakehouse/internal/config"
	"github.com/Grupo-Fixia/md-intake-lakehouse/internal/consumer"
	"github.com/Grupo-Fixia/md-intake-lakehouse/internal/storage"
	"github.com/Grupo-Fixia/md-intake-lakehouse/internal/validator"
)

func main() {
	log.Println("Iniciando servicio ms-intake-lakehouse...")

	cfg := config.Load()

	// 1. Inicializar validador de esquemas JSON (GC-436)
	val, err := validator.New()
	if err != nil {
		log.Fatalf("Fallo crítico al compilar esquemas de eventos: %v", err)
	}
	log.Println("Esquemas de contratos JSON compilados exitosamente.")

	// 2. Inicializar cliente de almacenamiento RustFS / S3
	store, err := storage.New(cfg)
	if err != nil {
		log.Fatalf("Fallo crítico al inicializar almacenamiento S3: %v", err)
	}

	initCtx, initCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := store.EnsureBucketExists(initCtx); err != nil {
		log.Printf("Advertencia al verificar bucket %s: %v", cfg.MinioBucket, err)
	}
	initCancel()

	// 3. Inicializar consumidor Kafka
	cons := consumer.New(cfg, store, val)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Iniciar consumidor en segundo plano
	go func() {
		if err := cons.Start(ctx); err != nil {
			log.Printf("Consumidor Kafka detenido con error: %v", err)
		}
	}()

	// 4. Servidor HTTP para Health Checks y Métricas
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"service":"ms-intake-lakehouse","status":"UP"}`))
	})

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		metrics := cons.GetMetrics()
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(metrics)
	})

	server := &http.Server{
		Addr:    ":" + cfg.HTTPPort,
		Handler: mux,
	}

	go func() {
		log.Printf("Servidor HTTP escuchando en puerto %s...", cfg.HTTPPort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Fallo en servidor HTTP: %v", err)
		}
	}()

	// Esperar señal de terminación elegante (Graceful Shutdown)
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	log.Println("Señal de apagado recibida, finalizando componentes...")
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = server.Shutdown(shutdownCtx)

	log.Println("ms-intake-lakehouse finalizado correctamente.")
}
