package clickhouse

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/pkg/errors"
	"github.com/rwynn/monstache/v6/pkg/sinks/bulk"
	"github.com/rwynn/monstache/v6/pkg/sinks/clickhouse/view"
	"github.com/sirupsen/logrus"
)

type Config struct {
	Enabled bool `toml:"enabled"`
	// example: http://localhost:8123
	Endpoint string `toml:"endpoint"`
	// localhost:9000
	EndpointTCP string `toml:"endpoint-tcp"`
	// Sets `input_format_skip_unknown_fields`, allowing ClickHouse to discard fields not present in the table schema.
	SkipUnknownFields bool `toml:"skip-unknown-fields"`
	// Sets `date_time_input_format` to `best_effort`, allowing ClickHouse to properly parse RFC3339/ISO 8601.
	DateTimeBestEffort bool `toml:"date-time-best-effort"`
	Auth               Auth `toml:"auth"`
	// Only one database per config
	Database string `toml:"database"`
	// prefix table name hkg_
	TablePrefix string `toml:"table-prefix"`
	// suffix table name, e.g., _v1
	TableSuffix string `toml:"table-suffix"`
	// enable http mode, show table views
	Http bool `toml:"http"`
}

// Auth
// support basic auth
// https://clickhouse.com/docs/en/interfaces/http#default-database
type Auth struct {
	// If the user name is not specified, the default name is used.
	User string `toml:"user"`
	// If the password is not specified, the empty password is used.
	Password string `toml:"password"`
}

type Client struct {
	httpClient *http.Client
	db         *sql.DB
	config     Config
	// note: 请在停止 monstache 后删除表结构
	tablesCache map[string]struct{}
	mu          sync.Mutex
	viewManager view.Manager
}

func (c *Client) EmbedDoc() bool {
	return true
}

func (c *Client) Name() string {
	return "clickhouse"
}

func (c *Client) Commit(ctx context.Context, requests []bulk.BulkableRequest) error {
	docsByTable := make(map[string][]interface{})
	var tables []string
	for _, request := range requests {
		ns := request.GetNamespace()
		table := view.ConvertToClickhouseTable(ns, c.config.TablePrefix, c.config.TableSuffix)
		if docs, ok := docsByTable[table]; ok {
			docsByTable[table] = append(docs, request.GetDoc())
		} else {
			docsByTable[table] = []interface{}{request.GetDoc()}
		}

		tables = append(tables, table)
		// collect view fields
		c.viewManager.Collect(table, request.GetDoc())
	}

	// make sure table exists
	if err := c.EnsureTableExists(ctx, tables); err != nil {
		return err
	}

	for table, docs := range docsByTable {
		if err := c.BatchInsert(ctx, c.config.Database, table, docs); err != nil {
			return err
		}
	}
	return nil
}

func NewClient(config Config) (*Client, view.Manager) {
	db := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{config.EndpointTCP},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: config.Auth.User,
			Password: config.Auth.Password,
		},
		// TLS: &tls.Config{
		// 	InsecureSkipVerify: true,
		// },
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout: time.Second * 30,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		Debug:                false,
		BlockBufferSize:      10,
		MaxCompressionBuffer: 10240,
	})
	db.SetMaxIdleConns(5)
	db.SetMaxOpenConns(10)
	db.SetConnMaxLifetime(time.Hour)

	var viewManager view.Manager
	if config.Http {
		viewManager = view.NewViewManager()
	} else {
		viewManager = &view.MockManager{}
	}
	viewManager.Start()

	return &Client{
		// fixme: a better settings
		httpClient:  http.DefaultClient,
		config:      config,
		db:          db,
		tablesCache: make(map[string]struct{}),
		viewManager: viewManager,
	}, viewManager
}

// BatchInsert
// https://clickhouse.com/docs/en/faq/integration/json-import
func (c *Client) BatchInsert(ctx context.Context, database, table string, rows []interface{}) error {
	// build request
	u, err := url.Parse(c.config.Endpoint)
	if err != nil {
		return errors.Wrap(err, "failed to parse baseURL")
	}

	params := url.Values{}
	params.Set("input_format_import_nested_json", "1")
	if c.config.SkipUnknownFields {
		params.Set("input_format_skip_unknown_fields", "1")
	}
	if c.config.DateTimeBestEffort {
		params.Set("date_time_input_format", "best_effort")
	}
	query := fmt.Sprintf("INSERT INTO `%s`.`%s` FORMAT JSONEachRow", database, table)
	params.Set("query", query)

	u.RawQuery = params.Encode()
	finalURL := u.String()
	logrus.Debugf("request URL: %s", finalURL)

	// compression
	// https://clickhouse.com/docs/en/interfaces/http#compression
	var buf bytes.Buffer
	gzipWriter := gzip.NewWriter(&buf)
	for _, user := range rows {
		jsonData, err := json.Marshal(user)
		if err != nil {
			return errors.Wrap(err, "Error marshal json")
		}
		_, err = gzipWriter.Write(jsonData)
		if err != nil {
			return errors.Wrap(err, "Error writing GZIP writer")
		}
		_, err = gzipWriter.Write([]byte("\n")) // Delimiter for JSONEachRow format
		if err != nil {
			return errors.Wrap(err, "Error writing GZIP writer")
		}
	}
	if err := gzipWriter.Close(); err != nil {
		return errors.Wrap(err, "Error closing GZIP writer")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", finalURL, &buf)
	if err != nil {
		return errors.Wrap(err, "failed to build request")
	}

	req.Header.Set("Content-Encoding", "gzip")
	// setup auth
	if c.config.Auth.User != "" {
		req.Header.Set("X-ClickHouse-User", c.config.Auth.User)
	}
	if c.config.Auth.Password != "" {
		req.Header.Set("X-ClickHouse-Key", c.config.Auth.Password)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "failed to post request to clickhouse")
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return errors.Wrap(err, "failed to parse response")
	}
	result := string(data)
	if resp.StatusCode != http.StatusOK {
		fmt.Printf("Error response from ClickHouse: %s\n", resp.Status)
		return fmt.Errorf("%s", result)
	} else {
		fmt.Println("Data uploaded successfully")
	}

	return nil
}
