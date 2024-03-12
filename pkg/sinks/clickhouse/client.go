package clickhouse

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"io"
	"net/http"
	"net/url"
)

type ClickHouseConfig struct {
	// example: http://localhost:8123
	Endpoint string
	// Sets `input_format_skip_unknown_fields`, allowing ClickHouse to discard fields not present in the table schema.
	SkipUnknownFields bool
	// Sets `date_time_input_format` to `best_effort`, allowing ClickHouse to properly parse RFC3339/ISO 8601.
	DateTimeBestEffort bool
	Auth               Auth
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
	config     ClickHouseConfig
}

func NewClient(config ClickHouseConfig) *Client {
	return &Client{
		// fixme: a better settings
		httpClient: http.DefaultClient,
		config:     config,
	}
}

// BatchInsert
// https://clickhouse.com/docs/en/faq/integration/json-import
func (c Client) BatchInsert(database, table string, rows []interface{}) error {
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

	req, err := http.NewRequest("POST", finalURL, &buf)
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
