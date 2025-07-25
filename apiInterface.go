package main

type ApiInterface interface {
	PlaceOrder() string
	GetBalance() (string, error)
}
