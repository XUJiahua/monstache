package clickhouse

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"net/http"
	"net/url"
)

type Auth struct {
	User     string
	Password string
}

type Client struct {
	httpClient *http.Client
	baseURL    string
}

func NewClient(endpoint string) *Client {
	return &Client{
		// fixme: a better settings
		httpClient: http.DefaultClient,
		baseURL:    endpoint,
	}
}

// BatchInsert
// https://clickhouse.com/docs/en/faq/integration/json-import
// reference
//
//	http://localhost:80/?\
//	                                    input_format_import_nested_json=1&\
//	                                    input_format_skip_unknown_fields=1&\
//	                                    date_time_input_format=best_effort&\
//	                                    query=INSERT+INTO+%22my_database%22.%22my_%5C%22table%5C%22%22+FORMAT+JSONAsObject"
func (c Client) BatchInsert(database, table string, rows []interface{}) error {
	u, err := url.Parse(c.baseURL)
	if err != nil {
		return errors.Wrap(err, "failed to parse baseURL")
	}
	params := url.Values{}
	query := fmt.Sprintf("INSERT INTO `%s`.`%s` FORMAT JSONEachRow", database, table)
	params.Set("query", query)
	//params.Set("") todo: add params for json compatible

	u.RawQuery = params.Encode()
	finalURL := u.String()

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
