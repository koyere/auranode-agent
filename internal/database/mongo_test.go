package database

import (
	"strings"
	"testing"

	"github.com/koyere/auranode-agent/pkg/proto"
)

func TestMongoURI(t *testing.T) {
	cases := []struct {
		name     string
		conn     proto.DBConn
		contains []string
		absent   []string
	}{
		{
			name:     "TCP sin auth",
			conn:     proto.DBConn{Engine: "mongodb", Host: "127.0.0.1", Port: 27017},
			contains: []string{"mongodb://127.0.0.1:27017/", "serverSelectionTimeoutMS=8000"},
			absent:   []string{"authSource", "@"},
		},
		{
			name:     "TCP con auth escapa credenciales",
			conn:     proto.DBConn{Engine: "mongodb", Host: "db", Port: 27017, User: "a b", Password: "p@ss:w/d"},
			contains: []string{"mongodb://a+b:p%40ss%3Aw%2Fd@db:27017/", "authSource=admin"},
		},
		{
			name:     "socket local URL-encoded",
			conn:     proto.DBConn{Engine: "mongodb", UseLocal: true, Socket: "/tmp/mongodb-27017.sock"},
			contains: []string{"mongodb://%2Ftmp%2Fmongodb-27017.sock/"},
		},
		{
			name:     "defaults host/puerto",
			conn:     proto.DBConn{Engine: "mongodb"},
			contains: []string{"mongodb://127.0.0.1:27017/"},
		},
	}
	for _, c := range cases {
		uri := mongoURI(c.conn)
		for _, sub := range c.contains {
			if !strings.Contains(uri, sub) {
				t.Errorf("%s: %q no contiene %q", c.name, uri, sub)
			}
		}
		for _, sub := range c.absent {
			if strings.Contains(uri, sub) {
				t.Errorf("%s: %q no debería contener %q", c.name, uri, sub)
			}
		}
	}
}

func TestMongoRole(t *testing.T) {
	for priv, want := range map[string]string{
		proto.DBPrivReadOnly:  "read",
		proto.DBPrivReadWrite: "readWrite",
		proto.DBPrivAll:       "dbOwner",
	} {
		got, err := mongoRole(priv)
		if err != nil || got != want {
			t.Errorf("mongoRole(%q)=%q,%v; quería %q", priv, got, err, want)
		}
	}
	if _, err := mongoRole("bogus"); err == nil {
		t.Error("esperaba error con privilegio desconocido")
	}
}

func TestMongoDumpNaming(t *testing.T) {
	if got := enginePrefix("mongodb"); got != "mongo" {
		t.Errorf("enginePrefix(mongodb)=%q, quería mongo", got)
	}
	if got := dumpSuffix("mongodb"); got != ".archive.gz" {
		t.Errorf("dumpSuffix(mongodb)=%q, quería .archive.gz", got)
	}
	name := "mongo_shop_20260714-190000.archive.gz"
	if !isDumpFile(name) {
		t.Errorf("isDumpFile(%q)=false", name)
	}
	engine, db := parseDumpName(name)
	if engine != "mongodb" || db != "shop" {
		t.Errorf("parseDumpName(%q)=(%q,%q), quería (mongodb,shop)", name, engine, db)
	}
	// BD con guiones bajos.
	if _, db := parseDumpName("mongo_my_shop_db_20260714-190000.archive.gz"); db != "my_shop_db" {
		t.Errorf("parseDumpName db con guiones bajos = %q", db)
	}
}
