package clickhouse

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/rwynn/monstache/v6/pkg/sinks/bulk"
	"github.com/sirupsen/logrus"
	"io"
	"net/http"
	"net/url"
)

type Config struct {
	Enabled bool
	// example: http://localhost:8123
	Endpoint string `toml:"endpoint"`
	// Sets `input_format_skip_unknown_fields`, allowing ClickHouse to discard fields not present in the table schema.
	SkipUnknownFields bool `toml:"skip_unknown_fields"`
	// Sets `date_time_input_format` to `best_effort`, allowing ClickHouse to properly parse RFC3339/ISO 8601.
	DateTimeBestEffort bool `toml:"date_time_best_effort"`
	Auth               Auth `toml:"auth"`
	// mongodb op namespace (database.collection) -> clickhouse namespace (database.table)
	Sinks map[string]Namespace `toml:"sinks"`
}

type Namespace struct {
	Database string `toml:"database"`
	Table    string `toml:"table"`
}

// Auth
// support basic auth
// https://clickhouse.com/docs/en/interfaces/http#default-database
type Auth struct {
	// If the user name is not specified, the default name is used.
	User string
	// If the password is not specified, the empty password is used.
	Password string
}

type Client struct {
	httpClient *http.Client
	config     Config
}

func (c Client) Commit(ctx context.Context, requests []bulk.BulkableRequest) error {
	docsByNS := make(map[string][]interface{})
	for _, request := range requests {
		ns := request.GetNamespace()
		if docs, ok := docsByNS[ns]; ok {
			docsByNS[ns] = append(docs, request.GetDoc())
		} else {
			docsByNS[ns] = []interface{}{request.GetDoc()}
		}
	}
	for ns, docs := range docsByNS {
		if target, ok := c.config.Sinks[ns]; ok {
			if err := c.BatchInsert(ctx, target.Database, target.Table, docs); err != nil {
				return err
			}
		} else {
			return fmt.Errorf("clickhouse sink is not properly set for namespace %s", ns)
		}
	}
	return nil
}

func NewClient(config Config) *Client {
	return &Client{
		// fixme: a better settings
		httpClient: http.DefaultClient,
		config:     config,
	}
}

// BatchInsert
// https://clickhouse.com/docs/en/faq/integration/json-import
func (c Client) BatchInsert(ctx context.Context, database, table string, rows []interface{}) error {
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
