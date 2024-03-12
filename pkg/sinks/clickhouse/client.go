package clickhouse

import (
	"bytes"
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
	// todo: support basic auth
	Auth Auth
}

type Auth struct {
	User     string
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

	var buf bytes.Buffer
	for _, user := range rows {
		jsonData, err := json.Marshal(user)
		if err != nil {
			return err
		}
		buf.Write(jsonData)
		buf.WriteString("\n") // Delimiter for JSONEachRow format
	}
	req, err := http.NewRequest("POST", finalURL, &buf)
	if err != nil {
		return errors.Wrap(err, "failed to build request")
	}
	//req.Header.Set("Content-Type", "application/json")
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
