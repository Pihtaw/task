package subpub

import (
	"context"
	"errors"
	"sync"
)

var ErrSubPubClosed = errors.New("task closed")

// MessageHandler – обработчик сообщения
type MessageHandler func(msg interface{})

// Subscription – интерфейс отписки
type Subscription interface {
	Unsubscribe()
}

// SubPub – интерфейс шины событий
type SubPub interface {
	// Subscribe создаёт асинхронного подписчика на subject
	Subscribe(subject string, cb MessageHandler) (Subscription, error)
	// Publish публикует сообщение msg на subject
	Publish(subject string, msg interface{}) error
	// Close завершает работу шины – может блокироваться до доставки данных, при отменённом контексте выходим сразу
	Close(ctx context.Context) error
}

// subscription реализует Subscription. Для каждого подписки запускается горутина, которая забирает сообщения из внутренней очереди.
type subscription struct {
	subject string
	bus     *subPubImpl
	cb      MessageHandler

	mu     sync.Mutex
	cond   *sync.Cond
	queue  []interface{}
	closed bool
	once   sync.Once
}

// конструктор подписки
func newSubscription(bus *subPubImpl, subject string, cb MessageHandler) *subscription {
	sub := &subscription{
		subject: subject,
		bus:     bus,
		cb:      cb,
		queue:   make([]interface{}, 0),
	}
	sub.cond = sync.NewCond(&sub.mu)
	return sub
}

// startDelivery – горутина доставки сообщений подписчику. Она гарантирует последовательное (FIFO) выполнение обработчика.
func (s *subscription) startDelivery(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		s.mu.Lock()
		// Ждем пока в очереди не появятся сообщения или будет установлен флаг closed
		for len(s.queue) == 0 && !s.closed {
			s.cond.Wait()
		}
		// Если очередь пуста и подписка закрыта – завершаем горутину
		if len(s.queue) == 0 && s.closed {
			s.mu.Unlock()
			return
		}
		msg := s.queue[0]
		s.queue = s.queue[1:]
		s.mu.Unlock()

		// Вызов обработчика
		s.cb(msg)
	}
}

// метод добавления сообщения в очередь подписки
func (s *subscription) enqueue(msg interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.queue = append(s.queue, msg)
	s.cond.Signal()
}

// Unsubscribe – отписка: закрываем очередь, сигналим горутине, удаляем подписку из шины.
func (s *subscription) Unsubscribe() {
	s.once.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.cond.Broadcast()
		s.mu.Unlock()
		s.bus.removeSubscription(s.subject, s)
	})
}

type subPubImpl struct {
	mu     sync.RWMutex
	subs   map[string]map[*subscription]struct{}
	closed bool
	wg     sync.WaitGroup
}

// NewSubPub – конструктор шины событий
func NewSubPub() SubPub {
	return &subPubImpl{
		subs: make(map[string]map[*subscription]struct{}),
	}
}

// Subscribe регистрирует подписчика на subject, запускает горутину доставки
func (bus *subPubImpl) Subscribe(subject string, cb MessageHandler) (Subscription, error) {
	bus.mu.Lock()
	defer bus.mu.Unlock()
	if bus.closed {
		return nil, ErrSubPubClosed
	}
	sub := newSubscription(bus, subject, cb)
	if bus.subs[subject] == nil {
		bus.subs[subject] = make(map[*subscription]struct{})
	}
	bus.subs[subject][sub] = struct{}{}
	bus.wg.Add(1)
	go sub.startDelivery(&bus.wg)
	return sub, nil
}

// удаление подписки из карты
func (bus *subPubImpl) removeSubscription(subject string, sub *subscription) {
	bus.mu.Lock()
	defer bus.mu.Unlock()
	if bus.subs[subject] != nil {
		delete(bus.subs[subject], sub)
		if len(bus.subs[subject]) == 0 {
			delete(bus.subs, subject)
		}
	}
}

// Publish отправляет сообщение каждому подписчику на указанный subject
func (bus *subPubImpl) Publish(subject string, msg interface{}) error {
	bus.mu.RLock()
	defer bus.mu.RUnlock()
	if bus.closed {
		return ErrSubPubClosed
	}
	if bus.subs[subject] == nil {
		// Нет подписчиков – ничего не делаем
		return nil
	}
	// Копируем подписчиков, чтобы не держать блокировку во время enqueue
	subs := make([]*subscription, 0, len(bus.subs[subject]))
	for sub := range bus.subs[subject] {
		subs = append(subs, sub)
	}
	for _, sub := range subs {
		sub.enqueue(msg)
	}
	return nil
}

// Close завершает работу шины. Мы отписываем всех подписчиков и ждём завершения горутин доставки.
// Если контекст отменён – выходим сразу, не блокируясь
func (bus *subPubImpl) Close(ctx context.Context) error {
	// Блокируем регистрацию новых подписок/публикаций
	bus.mu.Lock()
	if bus.closed {
		bus.mu.Unlock()
		return nil
	}
	bus.closed = true
	// Собираем все подписки
	allSubs := make([]*subscription, 0)
	for _, subsMap := range bus.subs {
		for sub := range subsMap {
			allSubs = append(allSubs, sub)
		}
	}
	// Очищаем карту подписок
	bus.subs = nil
	bus.mu.Unlock()
	// Отписываем всех – горутины доставки получат сигнал завершения работы
	for _, sub := range allSubs {
		sub.Unsubscribe()
	}
	// Ждём завершения горутин, но не бесконечно – если контекст отменён, завершаем работу
	done := make(chan struct{})
	go func() {
		bus.wg.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}
