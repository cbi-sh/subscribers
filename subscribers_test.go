package subscribers_test

import (
	"log"
	"math/rand"
	"testing"
	"time"

	"github.com/cbi-sh/subscribers"
)

func BenchmarkGet(b *testing.B) {
	b.ResetTimer()
	for msisdn := int64(0); msisdn < int64(b.N); msisdn++ {
		if _, err := subscribers.Get(msisdn); err != nil {
			log.Fatal(err)
		}
	}
}

func BenchmarkSet(b *testing.B) {
	b.ResetTimer()
	for msisdn := int64(0); msisdn < int64(b.N); msisdn++ {

		subscriber := &subscribers.Subscriber{
			Msisdn:         msisdn,
			Changedate:     time.Now(),
			Languagetype:   int8(rand.Int31n(6)),
			Migrationtype:  int8(rand.Int31n(2)),
			Operatortype:   int8(rand.Int31n(2)),
			Statetype:      int8(rand.Int31n(2)),
			Subscribertype: int8(rand.Int31n(2)),
		}

		if err := subscribers.Set(subscriber); err != nil {
			log.Fatal(err)
		}
	}
}
