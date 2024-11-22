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
	"regexp"
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
	// 所有匹配 regex 的 ns 的需要进行预处理
	PreprocessNsRegex string `toml:"preprocess-namespace-regex"`
	// 只预处理字符串，从 nil 转为 ""
	PreprocessStringOnly bool `toml:"preprocess-string-only"`
	// dump errors
	DumpOnError bool `toml:"dump-on-error"`
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

	preprocessNsRE *regexp.Regexp
}

func (c *Client) EmbedDoc() bool {
	return true
}

func (c *Client) Name() string {
	return "clickhouse"
}

func (c *Client) Commit(ctx context.Context, requests []bulk.BulkableRequest) error {
	// group docs by table
	docsByTable := make(map[string][]interface{})
	nsByTable := make(map[string]string)
	var tables []string
	for _, request := range requests {
		ns := request.GetNamespace()
		table := view.ConvertToClickhouseTable(ns, c.config.TablePrefix, c.config.TableSuffix)
		nsByTable[table] = ns

		if docs, ok := docsByTable[table]; ok {
			docsByTable[table] = append(docs, request.GetDoc())
		} else {
			docsByTable[table] = []interface{}{request.GetDoc()}
		}

		tables = append(tables, table)
		// collect view fields
		c.viewManager.Collect(fmt.Sprintf("%s.%s", c.config.Database, table), request.GetDoc())
	}

	// make sure table exists
	if err := c.EnsureTableExists(ctx, tables); err != nil {
		return err
	}

	for table, docs := range docsByTable {
		ns := nsByTable[table]
		database := c.config.Database
		preprocess := c.NeedPreprocess(ns)
		if preprocess {
			if err := c.BatchInsertWithPreprocess(ctx, database, table, docs); err != nil {
				if c.config.DumpOnError {
					Dump(ns, database, table, docs)
				}
				return err
			}
		} else {
			if err := c.BatchInsert(ctx, database, table, docs); err != nil {
				if c.config.DumpOnError {
					Dump(ns, database, table, docs)
				}
				return err
			}
		}
	}
	return nil
}

func NewClient(config Config) (*Client, view.Manager) {
	var preprocessNsRE *regexp.Regexp
	if config.PreprocessNsRegex != "" {
		preprocessNsRE = regexp.MustCompile(config.PreprocessNsRegex)
	}

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
		httpClient:     http.DefaultClient,
		config:         config,
		db:             db,
		tablesCache:    make(map[string]struct{}),
		viewManager:    viewManager,
		preprocessNsRE: preprocessNsRE,
	}, viewManager
}

func (c *Client) NeedPreprocess(ns string) bool {
	if c.preprocessNsRE != nil {
		return c.preprocessNsRE.MatchString(ns)
	}

	return false
}

// preprocessBatch 需要保证同一个批次下的数据结构一致
func preprocessBatch(rows []interface{}, logger *logrus.Entry, stringOnly bool) ([]map[string]interface{}, error) {
	var newRows []map[string]interface{}

	// collect fields
	traveler := view.NewMapTraveler(view.WithLogger(logger), view.WithStringOnly(stringOnly))
	for _, row := range rows {
		data, err := json.Marshal(row)
		if err != nil {
			return nil, err
		}
		var doc map[string]interface{}
		err = json.Unmarshal(data, &doc)
		if err != nil {
			return nil, err
		}

		traveler.Collect(doc)
		newRows = append(newRows, doc)
	}

	for _, row := range newRows {
		traveler.AssignDefaultValues(row)
	}

	return newRows, nil
}

// BatchInsertWithPreprocess wrapper of BatchInsert, preprocess rows before inserting
func (c *Client) BatchInsertWithPreprocess(ctx context.Context, database, table string, rows []interface{}) error {
	logger := logrus.WithFields(logrus.Fields{
		"database": database,
		"table":    table,
	})
	newRows, err := preprocessBatch(rows, logger, c.config.PreprocessStringOnly)
	if err != nil {
		return errors.Wrap(err, "failed to preprocess batch")
	}
	var docs []interface{}
	for _, doc := range newRows {
		docs = append(docs, doc)
	}

	return c.BatchInsert(ctx, database, table, docs)
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

	tableFullname := fmt.Sprintf("`%s`.`%s`", database, table)
	query := fmt.Sprintf("INSERT INTO %s FORMAT JSONEachRow", tableFullname)
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
		logrus.Warnf("TBALE [%s] Error response from ClickHouse: %s", tableFullname, resp.Status)
		return fmt.Errorf("TABLE [%s]%s", tableFullname, result)
	}

	logrus.Debugf("Data uploaded successfully")
	return nil
}
