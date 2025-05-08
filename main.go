package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"

	"subpub/subpub"
	pb "subpub/subpub/pb"
)

// serverConfig содержит конфигурационные параметры сервиса
type serverConfig struct {
	GRPCPort string
}

func loadConfig() *serverConfig {
	port := flag.String("port", "50051", "gRPC server port")
	flag.Parse()
	return &serverConfig{
		GRPCPort: *port,
	}
}

// pubSubServer – реализация gRPC‑сервиса, использующая subpub для доставки событий
type pubSubServer struct {
	pb.UnimplementedPubSubServer
	bus subpub.SubPub
}

// NewPubSubServer создаёт новый gRPC‑сервис с зависимостью от шины событий
func NewPubSubServer(bus subpub.SubPub) pb.PubSubServer {
	return &pubSubServer{bus: bus}
}

// Subscribe – при получении запроса подписки создаётся подписка в шине
// Обработчик получает сообщения (ожидается, что msg – строка) и отправляет их клиенту,
// используя stream.Send. При ошибке отправки производится отписка
func (s *pubSubServer) Subscribe(req *pb.SubscribeRequest, stream pb.PubSub_SubscribeServer) error {
	log.Printf("Новая подписка для ключа: %s", req.Key)
	var subscription subpub.Subscription
	var err error

	subscription, err = s.bus.Subscribe(req.Key, func(msg interface{}) {
		// Приводим к строке и отправляем событие клиенту
		event := &pb.Event{Data: msg.(string)}
		if err := stream.Send(event); err != nil {
			log.Printf("Ошибка отправки события: %v", err)
			subscription.Unsubscribe()
		}
	})
	if err != nil {
		log.Printf("Ошибка подписки: %v", err)
		return status.Errorf(codes.Internal, "Subscribe error: %v", err)
	}
	// Блокируем до закрытия контекста (например, при отключении клиента)
	<-stream.Context().Done()
	subscription.Unsubscribe()
	log.Printf("Подписка для ключа %s завершена", req.Key)
	return nil
}

// Publish – публикует событие в шине
func (s *pubSubServer) Publish(ctx context.Context, req *pb.PublishRequest) (*emptypb.Empty, error) {
	log.Printf("Публикация события по ключу: %s", req.Key)
	err := s.bus.Publish(req.Key, req.Data)
	if err != nil {
		log.Printf("Ошибка публикации: %v", err)
		return nil, status.Errorf(codes.Internal, "Publish error: %v", err)
	}
	return &emptypb.Empty{}, nil

}

func main() {
	config := loadConfig()
	log.Printf("Запуск PubSub gRPC сервиса на порту %s", config.GRPCPort)

	// Создаём шину событий
	bus := subpub.NewSubPub()

	// Создаём gRPC сервер и регистрируем реализацию сервиса
	grpcServer := grpc.NewServer()
	pb.RegisterPubSubServer(grpcServer, NewPubSubServer(bus))

	// Открываем TCP‑слушатель
	lis, err := net.Listen("tcp", ":"+config.GRPCPort)
	if err != nil {
		log.Fatalf("Не удалось открыть порт: %v", err)
	}

	// Обработка graceful shutdown – при получении SIGINT/SIGTERM вызываем остановку сервера и шины
	go func() {
		sigC := make(chan os.Signal, 1)
		signal.Notify(sigC, syscall.SIGINT, syscall.SIGTERM)
		sig := <-sigC
		log.Printf("Получен сигнал %v. Завершаем работу...", sig)
		grpcServer.GracefulStop()
		// Завершаем шину с таймаутом
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := bus.Close(ctx); err != nil {
			log.Printf("Ошибка закрытия шины: %v", err)
		}
	}()

	// Запускаем сервер
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Ошибка работы gRPC сервера: %v", err)
	}
	log.Printf("Сервер остановлен")
}
