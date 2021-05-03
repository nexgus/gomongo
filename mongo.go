package gomongo

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type key int

const (
	keyPrincipalID key = iota
)

type MongoDB struct {
	ctx     context.Context
	connstr string
	client  *mongo.Client
	db      *mongo.Database
	coll    map[string]*mongo.Collection
}

// Open create an instance to keep MongoDB info and connects to server.
// If names is specfied, the first one is database name, and others are
// collection names.
func Open(
	ctx context.Context,
	addr string,
	appname string,
	names ...string,
) (*MongoDB, error) {
	m := &MongoDB{
		ctx: context.WithValue(
			ctx,
			keyPrincipalID,
			primitive.NewObjectID().Hex(),
		),
		connstr: fmt.Sprintf(
			"mongodb://%s/?readPreference=primary&appname=%s",
			addr, appname,
		),
		coll: make(map[string]*mongo.Collection),
	}

	opts := options.Client().ApplyURI(m.connstr)
	client, err := mongo.NewClient(opts)
	if err != nil {
		return nil, err
	}
	m.client = client

	if err := m.Connect(); err != nil {
		return nil, err
	}

	if len(names) > 0 {
		m.SetDatabase(names[0])
	}

	if len(names) > 1 {
		m.SetCollections(names[1:]...)
	}

	return m, nil
}

// Client returns the mongodb client.
func (m *MongoDB) Client() *mongo.Client {
	return m.client
}

// Close disconnects the connection to the server.
func (m *MongoDB) Close() error {
	return m.client.Disconnect(m.ctx)
}

// Collection returns a specified collection.
func (m *MongoDB) Collection(name string) (*mongo.Collection, error) {
	coll, ok := m.coll[name]
	if !ok {
		return nil, fmt.Errorf("not defined collection %s", name)
	}

	return coll, nil
}

// Collections returns all collections.
func (m *MongoDB) Collections() map[string]*mongo.Collection {
	return m.coll
}

// CollectionAreExisting return a boolt to indicate if all collections are existing.
func (m *MongoDB) CollectionsAreExisting() bool {
	if m.client == nil {
		return false
	}
	if m.db == nil {
		return false
	}
	if m.coll == nil {
		return false
	}
	if len(m.coll) == 0 {
		return false
	}

	colls, err := m.db.ListCollectionNames(m.ctx, bson.M{})
	if err != nil {
		return false
	}
	for name := range m.coll {
		isPresent := inStringSlice(name, colls)
		if !isPresent {
			return false
		}
	}
	return true
}

// Connect establish a connection to server.
func (m *MongoDB) Connect() error {
	return m.client.Connect(m.ctx)
}

// Context returns the context.
func (m *MongoDB) Context() context.Context {
	return m.ctx
}

// CreateIndex creates an index on collection name.
func (m *MongoDB) CreateIndex(name, key string, order int) (string, error) {
	coll, ok := m.coll[name]
	if !ok {
		return "", fmt.Errorf("not defined collection %s", name)
	}

	asscending := 1
	if order == -1 {
		asscending = -1
	}

	model := mongo.IndexModel{
		Keys: bson.D{{Key: key, Value: asscending}},
		//Options: options.Index().SetBackground(true),
	}

	opts := options.CreateIndexes().SetMaxTime(2 * time.Second)

	return coll.Indexes().CreateOne(m.ctx, model, opts)
}

// Database returns the database.
func (m *MongoDB) Database() (*mongo.Database, error) {
	if m.db == nil {
		return nil, fmt.Errorf("database is not defined")
	}

	return m.db, nil
}

// DatabaseIsExisting return a bool to indicate if a database is existing.
func (m *MongoDB) DatabaseIsExisting() bool {
	if m.client == nil {
		return false
	}
	if m.db == nil {
		return false
	}

	name := m.db.Name()
	databases, _ := m.client.ListDatabaseNames(m.ctx, bson.M{})
	return inStringSlice(name, databases)
}

// DeleteOne executes a delete command to delete at most one document from
// the specified collection.
func (m *MongoDB) DeleteOne(name string, filter interface{}) (int64, error) {
	coll, err := m.selCollection(name)
	if err != nil {
		return -1, err
	}

	result, err := coll.DeleteOne(m.ctx, filter)
	if err != nil {
		return -1, err
	}

	return result.DeletedCount, nil
}

// EstimatedDocumentCount gets an estimated of the number of documents in the
// specified collection.
func (m *MongoDB) EstimatedDocumentCount(name string) (int64, error) {
	coll, err := m.selCollection(name)
	if err != nil {
		return -1, err
	}

	// specify the MaxTime option to limit the amount of time the operation
	// can run on the server
	opts := options.EstimatedDocumentCount().SetMaxTime(2 * time.Second)

	return coll.EstimatedDocumentCount(m.ctx, opts)
}

// Find stores the matching documents from the specified collection in a list.
func (m *MongoDB) Find(
	name string,
	filter interface{},
	docs *[]map[string]interface{},
	opts ...*options.FindOptions,
) error {
	coll, err := m.selCollection(name)
	if err != nil {
		return err
	}

	var cursor *mongo.Cursor
	if len(opts) == 0 {
		cursor, err = coll.Find(m.ctx, filter)
	} else {
		cursor, err = coll.Find(m.ctx, filter, opts[0])
	}
	if err != nil {
		return err
	}

	return cursor.All(m.ctx, docs)
}

// FindOne stores the matching document from the specified collection in an
// interface.
func (m *MongoDB) FindOne(
	name string,
	filter interface{},
	doc interface{},
) error {
	coll, err := m.selCollection(name)
	if err != nil {
		return err
	}

	return coll.FindOne(m.ctx, filter).Decode(doc)
}

// InsertMany executes an insert command to insert multiple documents into the
// specified collection.
func (m *MongoDB) InsertMany(name string, docs []interface{}) error {
	coll, err := m.selCollection(name)
	if err != nil {
		return err
	}

	_, err = coll.InsertMany(m.ctx, docs)
	return err
}

// InsertOne executes an insert command to insert a single document into the
// specified collection.
func (m *MongoDB) InsertOne(name string, doc interface{}) error {
	coll, err := m.selCollection(name)
	if err != nil {
		return err
	}

	_, err = coll.InsertOne(m.ctx, doc)
	return err
}

// ReplaceOne executes an update command to replace at most one document in
// the specified collection. If upsert is true, a new document will be
// inserted if the filter does not match any documents in the collection.
func (m *MongoDB) ReplaceOne(
	name string,
	filter interface{},
	doc interface{},
	upsert bool,
) (bool, error) {
	var created bool
	coll, err := m.selCollection(name)
	if err != nil {
		return created, err
	}

	opts := options.Replace().SetUpsert(upsert)
	result, err := coll.ReplaceOne(m.ctx, filter, doc, opts)
	if err != nil {
		return created, err
	}

	if result.UpsertedID != nil {
		created = true
	}
	return created, nil
}

// Ping sends a ping command to verify that the client can connect to the
// deployment.
func (m *MongoDB) Ping() error {
	return m.client.Ping(m.ctx, nil)
}

// SetDatabase configures the database.
func (m *MongoDB) SetDatabase(name string) {
	m.db = m.client.Database(name)
}

// SetCollections sets one or more collections.
func (m *MongoDB) SetCollections(names ...string) error {
	if m.db == nil {
		return fmt.Errorf("database is not configured yet")
	}

	for _, name := range names {
		m.coll[name] = m.db.Collection(name)
	}

	return nil
}

func (m *MongoDB) selCollection(name string) (*mongo.Collection, error) {
	var (
		coll *mongo.Collection
		err  error
	)

	if len(name) == 0 {
		if len(m.coll) != 1 {
			err = fmt.Errorf("must assign collection name while registered collection count is not 1 (one)")
		} else {
			for _, coll = range m.coll {
				break
			}
		}
	} else {
		if c, ok := m.coll[name]; !ok {
			err = fmt.Errorf("not defined collection %s", name)
		} else {
			coll = c
		}
	}

	return coll, err
}
