// Package buffer persiste métricas offline con bbolt para enviarlas al reconectar.
package buffer

import (
	"encoding/binary"
	"encoding/json"
	"time"

	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"

	"github.com/koyere/auranode-agent/pkg/proto"
)

const (
	bucketMetrics = "metrics"
	maxEntries    = 2000 // ~2000 snapshots ≈ 33 min a 1/seg
)

type Buffer struct {
	db  *bolt.DB
	log *zap.Logger
}

func Open(path string, log *zap.Logger) (*Buffer, error) {
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return nil, err
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketMetrics))
		return err
	}); err != nil {
		db.Close()
		return nil, err
	}
	return &Buffer{db: db, log: log}, nil
}

func (b *Buffer) Close() { b.db.Close() }

// Push guarda una métrica en el buffer. Si supera maxEntries, descarta la más antigua.
func (b *Buffer) Push(m proto.Metrics) {
	data, err := json.Marshal(m)
	if err != nil {
		return
	}
	b.db.Update(func(tx *bolt.Tx) error { //nolint:errcheck
		bkt := tx.Bucket([]byte(bucketMetrics))
		seq, _ := bkt.NextSequence()
		key := itob(seq)

		// Límite de tamaño: eliminar la más vieja si excede
		for bkt.Stats().KeyN >= maxEntries {
			cur := bkt.Cursor()
			k, _ := cur.First()
			if k != nil {
				bkt.Delete(k) //nolint:errcheck
			}
		}

		return bkt.Put(key, data)
	})
}

// Drain devuelve todas las métricas almacenadas y las elimina.
func (b *Buffer) Drain() []proto.Metrics {
	var out []proto.Metrics
	b.db.Update(func(tx *bolt.Tx) error { //nolint:errcheck
		bkt := tx.Bucket([]byte(bucketMetrics))
		bkt.ForEach(func(k, v []byte) error { //nolint:errcheck
			var m proto.Metrics
			if json.Unmarshal(v, &m) == nil {
				out = append(out, m)
			}
			return nil
		})
		// Borrar todo el bucket y recrearlo
		tx.DeleteBucket([]byte(bucketMetrics))    //nolint:errcheck
		tx.CreateBucket([]byte(bucketMetrics))     //nolint:errcheck
		return nil
	})
	return out
}

// Count retorna cuántas métricas hay en el buffer.
func (b *Buffer) Count() int {
	var n int
	b.db.View(func(tx *bolt.Tx) error { //nolint:errcheck
		bkt := tx.Bucket([]byte(bucketMetrics))
		if bkt != nil {
			n = bkt.Stats().KeyN
		}
		return nil
	})
	return n
}

func itob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}
