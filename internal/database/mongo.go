package database

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strconv"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/koyere/auranode-agent/pkg/proto"
)

// Cliente de MongoDB (Parte 3 · motor documental). El agente se conecta como cliente con
// el driver oficial para explorar (BDs → colecciones + estado + usuarios) y gestionar
// (crear/eliminar BD y usuario, contraseña, roles). Los backups usan mongodump/mongorestore
// (ver backup.go). Nunca administra el sistema.

// mongoURI construye la cadena de conexión. Con UseLocal/Socket usa el socket unix local
// (ruta URL-encoded); si no, TCP con credenciales. authSource=admin (convención habitual).
func mongoURI(c proto.DBConn) string {
	userinfo := ""
	if c.User != "" {
		userinfo = url.QueryEscape(c.User)
		if c.Password != "" {
			userinfo += ":" + url.QueryEscape(c.Password)
		}
		userinfo += "@"
	}
	host := ""
	if c.UseLocal || c.Socket != "" {
		sock := c.Socket
		if sock == "" {
			sock = "/tmp/mongodb-27017.sock"
		}
		host = url.QueryEscape(sock)
	} else {
		h := c.Host
		if h == "" {
			h = "127.0.0.1"
		}
		port := c.Port
		if port == 0 {
			port = 27017
		}
		host = fmt.Sprintf("%s:%d", h, port)
	}
	params := "?connectTimeoutMS=8000&serverSelectionTimeoutMS=8000"
	if c.User != "" {
		params += "&authSource=admin"
	}
	return fmt.Sprintf("mongodb://%s%s/%s", userinfo, host, params)
}

func mongoConnect(ctx context.Context, c proto.DBConn) (*mongo.Client, error) {
	cli, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI(c)))
	if err != nil {
		return nil, err
	}
	if err := cli.Ping(ctx, nil); err != nil {
		_ = cli.Disconnect(ctx)
		return nil, err
	}
	return cli, nil
}

func (m *Manager) testMongo(ctx context.Context, conn proto.DBConn) (json.RawMessage, error) {
	cli, err := mongoConnect(ctx, conn)
	if err != nil {
		return nil, err
	}
	defer cli.Disconnect(ctx)
	ver := mongoVersion(ctx, cli)
	return marshal(map[string]string{"version": "MongoDB " + ver})
}

// mongoVersion lee la versión del servidor (buildInfo). Best-effort.
func mongoVersion(ctx context.Context, cli *mongo.Client) string {
	var res bson.M
	if err := cli.Database("admin").RunCommand(ctx, bson.D{{Key: "buildInfo", Value: 1}}).Decode(&res); err == nil {
		if v, ok := res["version"].(string); ok {
			return v
		}
	}
	return ""
}

func (m *Manager) databasesMongo(ctx context.Context, conn proto.DBConn) (json.RawMessage, error) {
	cli, err := mongoConnect(ctx, conn)
	if err != nil {
		return nil, err
	}
	defer cli.Disconnect(ctx)

	out := proto.DBDatabasesData{Databases: []proto.DBInfo{}, Users: []proto.DBUser{}}
	out.Status = proto.DBEngineStatus{Engine: "mongodb", Version: "MongoDB " + mongoVersion(ctx, cli)}

	// Estado del motor (serverStatus).
	var ss bson.M
	if err := cli.Database("admin").RunCommand(ctx, bson.D{{Key: "serverStatus", Value: 1}}).Decode(&ss); err == nil {
		out.Status.UptimeSec = toInt64(ss["uptime"])
		if conns, ok := ss["connections"].(bson.M); ok {
			out.Status.Connections = int(toInt64(conns["current"]))
		}
	}

	// Bases de datos con tamaño en disco.
	res, err := cli.ListDatabases(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	for _, d := range res.Databases {
		out.Databases = append(out.Databases, proto.DBInfo{Name: d.Name, SizeBytes: d.SizeOnDisk})
	}

	// Usuarios (admin.system.users). Best-effort: requiere privilegios de lectura en admin.
	if cur, err := cli.Database("admin").Collection("system.users").Find(ctx, bson.D{}); err == nil {
		var users []bson.M
		if cur.All(ctx, &users) == nil {
			for _, u := range users {
				name, _ := u["user"].(string)
				dbn, _ := u["db"].(string)
				if name == "" {
					continue
				}
				out.Users = append(out.Users, proto.DBUser{
					Name: name + "@" + dbn, CanLogin: true, Privileges: mongoRolesSummary(u["roles"]),
				})
			}
		}
	}
	return marshal(out)
}

// mongoRolesSummary resume la lista de roles de un usuario ("role@db, ...").
func mongoRolesSummary(v any) string {
	arr, ok := v.(bson.A)
	if !ok {
		return ""
	}
	parts := []string{}
	for _, r := range arr {
		if rm, ok := r.(bson.M); ok {
			role, _ := rm["role"].(string)
			if role != "" {
				parts = append(parts, role)
			}
		}
	}
	sort.Strings(parts)
	if len(parts) > 4 {
		parts = append(parts[:4], "…")
	}
	return join(parts, ", ")
}

func (m *Manager) tablesMongo(ctx context.Context, conn proto.DBConn, database string) (json.RawMessage, error) {
	if !validIdent(database) {
		return nil, fmt.Errorf("db: nombre de base de datos no válido")
	}
	cli, err := mongoConnect(ctx, conn)
	if err != nil {
		return nil, err
	}
	defer cli.Disconnect(ctx)

	dbh := cli.Database(database)
	names, err := dbh.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	tables := []proto.DBTable{}
	for _, name := range names {
		t := proto.DBTable{Name: name}
		var stats bson.M
		if err := dbh.RunCommand(ctx, bson.D{{Key: "collStats", Value: name}}).Decode(&stats); err == nil {
			t.RowsEst = toInt64(stats["count"])
			t.SizeBytes = toInt64(stats["storageSize"])
		} else if n, e := dbh.Collection(name).EstimatedDocumentCount(ctx); e == nil {
			t.RowsEst = n
		}
		tables = append(tables, t)
	}
	sort.Slice(tables, func(i, j int) bool { return tables[i].SizeBytes > tables[j].SizeBytes })
	return marshal(tables)
}

// ─── Gestión (D3 para Mongo) ──────────────────────────────────────────────────

// mongoRole mapea el nivel de privilegio del panel a un rol de MongoDB.
func mongoRole(priv string) (string, error) {
	switch priv {
	case proto.DBPrivReadOnly:
		return "read", nil
	case proto.DBPrivReadWrite:
		return "readWrite", nil
	case proto.DBPrivAll:
		return "dbOwner", nil
	default:
		return "", fmt.Errorf("db: privilegio no soportado: %q", priv)
	}
}

func (m *Manager) adminMongo(ctx context.Context, conn proto.DBConn, spec proto.DBAdminSpec) (json.RawMessage, error) {
	cli, err := mongoConnect(ctx, conn)
	if err != nil {
		return nil, err
	}
	defer cli.Disconnect(ctx)

	authDB := spec.Database
	if authDB == "" {
		authDB = "admin"
	}

	switch spec.Action {
	case proto.DBAdminCreateDatabase:
		if !validIdent(spec.Database) {
			return nil, fmt.Errorf("db: nombre de base de datos no válido")
		}
		// Mongo crea la BD de forma perezosa: se materializa creando una colección inicial.
		if err := cli.Database(spec.Database).CreateCollection(ctx, "data"); err != nil {
			return nil, err
		}
		return adminResult(fmt.Sprintf("Base de datos %q creada (con una colección inicial 'data').", spec.Database))

	case proto.DBAdminDropDatabase:
		if !validIdent(spec.Database) {
			return nil, fmt.Errorf("db: nombre de base de datos no válido")
		}
		if err := cli.Database(spec.Database).Drop(ctx); err != nil {
			return nil, err
		}
		return adminResult(fmt.Sprintf("Base de datos %q eliminada.", spec.Database))

	case proto.DBAdminCreateUser:
		if !validIdent(spec.Username) || !validPassword(spec.Password) {
			return nil, fmt.Errorf("db: usuario o contraseña no válidos")
		}
		role := "readWrite"
		if spec.Privilege != "" {
			r, err := mongoRole(spec.Privilege)
			if err != nil {
				return nil, err
			}
			role = r
		}
		roleDB := spec.Database
		if roleDB == "" {
			roleDB = authDB
		}
		cmd := bson.D{
			{Key: "createUser", Value: spec.Username},
			{Key: "pwd", Value: spec.Password},
			{Key: "roles", Value: bson.A{bson.D{{Key: "role", Value: role}, {Key: "db", Value: roleDB}}}},
		}
		if err := cli.Database(authDB).RunCommand(ctx, cmd).Err(); err != nil {
			return nil, err
		}
		return adminResult(fmt.Sprintf("Usuario %q creado en %q (rol %s sobre %q).", spec.Username, authDB, role, roleDB))

	case proto.DBAdminDropUser:
		if !validIdent(spec.Username) {
			return nil, fmt.Errorf("db: nombre de usuario no válido")
		}
		if err := cli.Database(authDB).RunCommand(ctx, bson.D{{Key: "dropUser", Value: spec.Username}}).Err(); err != nil {
			return nil, err
		}
		return adminResult(fmt.Sprintf("Usuario %q eliminado.", spec.Username))

	case proto.DBAdminChangePassword:
		if !validIdent(spec.Username) || !validPassword(spec.Password) {
			return nil, fmt.Errorf("db: usuario o contraseña no válidos")
		}
		cmd := bson.D{{Key: "updateUser", Value: spec.Username}, {Key: "pwd", Value: spec.Password}}
		if err := cli.Database(authDB).RunCommand(ctx, cmd).Err(); err != nil {
			return nil, err
		}
		return adminResult(fmt.Sprintf("Contraseña de %q actualizada.", spec.Username))

	case proto.DBAdminGrant:
		if !validIdent(spec.Username) || !validIdent(spec.Database) {
			return nil, fmt.Errorf("db: usuario o base de datos no válidos")
		}
		role, err := mongoRole(spec.Privilege)
		if err != nil {
			return nil, err
		}
		cmd := bson.D{
			{Key: "grantRolesToUser", Value: spec.Username},
			{Key: "roles", Value: bson.A{bson.D{{Key: "role", Value: role}, {Key: "db", Value: spec.Database}}}},
		}
		if err := cli.Database(authDB).RunCommand(ctx, cmd).Err(); err != nil {
			return nil, err
		}
		return adminResult(fmt.Sprintf("Rol %s concedido a %q sobre %q.", role, spec.Username, spec.Database))

	case proto.DBAdminRevoke:
		if !validIdent(spec.Username) || !validIdent(spec.Database) {
			return nil, fmt.Errorf("db: usuario o base de datos no válidos")
		}
		// Revoca los roles básicos sobre la BD (los que puede haber concedido el panel).
		roles := bson.A{}
		for _, rn := range []string{"read", "readWrite", "dbOwner"} {
			roles = append(roles, bson.D{{Key: "role", Value: rn}, {Key: "db", Value: spec.Database}})
		}
		cmd := bson.D{{Key: "revokeRolesFromUser", Value: spec.Username}, {Key: "roles", Value: roles}}
		if err := cli.Database(authDB).RunCommand(ctx, cmd).Err(); err != nil {
			return nil, err
		}
		return adminResult(fmt.Sprintf("Roles revocados a %q sobre %q.", spec.Username, spec.Database))
	}
	return nil, fmt.Errorf("db: acción no soportada: %q", spec.Action)
}

// toInt64 convierte los números de BSON (int32/int64/float64) a int64.
func toInt64(v any) int64 {
	switch n := v.(type) {
	case int32:
		return int64(n)
	case int64:
		return n
	case float64:
		return int64(n)
	case string:
		x, _ := strconv.ParseInt(n, 10, 64)
		return x
	}
	return 0
}

// join une cadenas (evita importar strings solo para esto en este archivo).
func join(parts []string, sep string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += sep
		}
		out += p
	}
	return out
}
