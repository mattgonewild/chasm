package chasm

import (
	"errors"
	"iter"
	"sync"

	"github.com/mattgonewild/common"
)

type (
	Registry[T any] interface {
		Claim(key Key) (T, bool)
		Register(key Key, value T) error
		Release(key Key) error
		Get(key Key) (T, error)
	}

	BookEventLogRegistry     = Registry[common.Log[BookEvent]]
	CandleEventLogRegistry   = Registry[common.Log[CandleEvent]]
	TradeEventLogRegistry    = Registry[common.Log[TradeEvent]]
	SymbolEventLogRegistry   = Registry[common.Log[SymbolEvent]]
	ScheduleEventLogRegistry = Registry[common.Log[ScheduleEvent]]

	BrokerageDataEventLogRegistry interface {
		Book() BookEventLogRegistry
		Candle() CandleEventLogRegistry
		Trade() TradeEventLogRegistry
		Symbol() SymbolEventLogRegistry
		Schedule() ScheduleEventLogRegistry
	}
)

type registryEntry[T any] struct {
	claimed bool
	value   T
}

type CoarseRegistry[T any] struct {
	mu    sync.RWMutex
	entry map[Key]registryEntry[T]
	_     [32]byte
}

var (
	ErrRegistryConflict = errors.New("matt::chasm::registry: key conflict")
	ErrRegistryNotFound = errors.New("matt::chasm::registry: not found")
	ErrRegistryLocked   = errors.New("matt::chasm::registry: locked")
)

func NewCoarseRegistry[T any](initCap int) *CoarseRegistry[T] {
	return &CoarseRegistry[T]{
		entry: make(map[Key]registryEntry[T], initCap),
	}
}

func (this *CoarseRegistry[T]) Claim(key Key) (T, bool) {
	this.mu.Lock()
	entry, ok := this.entry[key]
	if !ok || entry.claimed {
		this.mu.Unlock()
		return entry.value, false
	}

	entry.claimed = true
	this.entry[key] = entry
	this.mu.Unlock()
	return entry.value, true
}

func (this *CoarseRegistry[T]) Register(key Key, value T) error {
	this.mu.Lock()
	_, ok := this.entry[key]
	if ok {
		this.mu.Unlock()
		return ErrRegistryConflict
	}

	this.entry[key] = registryEntry[T]{claimed: true, value: value}
	this.mu.Unlock()
	return nil
}

func (this *CoarseRegistry[T]) Release(key Key) error {
	this.mu.Lock()
	entry, ok := this.entry[key]
	if !ok {
		this.mu.Unlock()
		return ErrRegistryNotFound
	}

	entry.claimed = false
	this.entry[key] = entry
	this.mu.Unlock()
	return nil
}

func (this *CoarseRegistry[T]) Get(key Key) (T, error) {
	this.mu.RLock()
	entry, ok := this.entry[key]
	this.mu.RUnlock()
	if !ok {
		return entry.value, ErrRegistryNotFound
	}
	return entry.value, nil
}

func (this *CoarseRegistry[T]) Delete(key Key) error {
	this.mu.Lock()
	entry, ok := this.entry[key]
	if !ok {
		this.mu.Unlock()
		return ErrRegistryNotFound
	}

	if entry.claimed {
		this.mu.Unlock()
		return ErrRegistryLocked
	}

	delete(this.entry, key)
	this.mu.Unlock()
	return nil
}

func (this *CoarseRegistry[T]) ForEach(yield func(Key, T) bool) {
	this.mu.RLock()
	for key, entry := range this.entry {
		if !yield(key, entry.value) {
			break
		}
	}

	this.mu.RUnlock()
}

func (this *CoarseRegistry[T]) All() iter.Seq2[Key, T] {
	return func(yield func(Key, T) bool) {
		this.mu.RLock()
		for key, entry := range this.entry {
			if !yield(key, entry.value) {
				break
			}
		}

		this.mu.RUnlock()
	}
}

type CoarseBrokerageDataEventLogRegistry struct {
	book     CoarseRegistry[common.Log[BookEvent]]
	candle   CoarseRegistry[common.Log[CandleEvent]]
	trade    CoarseRegistry[common.Log[TradeEvent]]
	symbol   CoarseRegistry[common.Log[SymbolEvent]]
	schedule CoarseRegistry[common.Log[ScheduleEvent]]
}

func (this *CoarseBrokerageDataEventLogRegistry) Book() BookEventLogRegistry     { return &this.book }
func (this *CoarseBrokerageDataEventLogRegistry) Candle() CandleEventLogRegistry { return &this.candle }
func (this *CoarseBrokerageDataEventLogRegistry) Trade() TradeEventLogRegistry   { return &this.trade }
func (this *CoarseBrokerageDataEventLogRegistry) Symbol() SymbolEventLogRegistry { return &this.symbol }
func (this *CoarseBrokerageDataEventLogRegistry) Schedule() ScheduleEventLogRegistry {
	return &this.schedule
}
