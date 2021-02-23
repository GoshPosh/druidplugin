package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/bitly/go-simplejson"
	"golang.org/x/net/context"

	"github.com/grafana/grafana_plugin_model/go/datasource"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
)

type DruidDatasource struct {
	plugin.NetRPCUnsupportedPlugin
	logger hclog.Logger
}

//Request and Response Struct

type Result struct {
	Value float64
}

type Timestamp string

type Aggregation struct {
	Type      string `json:"type"`
	FieldName string `json:"fieldName,omitempty"`
	Name      string `json:"name"`
}

type Fields struct {
	FieldsType string   `json:"type"`
	Dimension  string   `json:"dimension"`
	Value      string   `json:"value"`
	Pattern    string   `json:"pattern"`
	InValues   []string `json:"values"`
}

type Filter struct {
	Type      string   `json:"type,omitempty"`
	Dimension string   `json:"dimension,omitempty"`
	Value     string   `json:"value,omitempty"`
	Pattern   string   `json:"pattern,omitempty"`
	InValues  []string `json:"values,omitempty"`
	Fields    []Fields `json:"fields,omitempty"`
}

type TimeSeriesPayload struct {
	QueryType    string        `json:"queryType"`
	DataSource   string        `json:"dataSource"`
	Granularity  string        `json:"granularity"`
	Aggregations []Aggregation `json:"aggregations"`
	Intervals    []string      `json:"intervals"`
	Filters      Filter        `json:"filter,omitempty"`
}

type Column struct {
	Dimension string `json:"dimension,omitempty"`
	Direction string `json:"direction,omitempty"`
}

type LimitSpec struct {
	Type    string   `json:"type,omitempty"`
	Limit   int      `json:"limit,omitempty"`
	Columns []Column `json:"columns,omitempty"`
}

type GroupByPayload struct {
	QueryType    string        `json:"queryType"`
	DataSource   string        `json:"dataSource"`
	Dimensions   []interface{} `json:"dimensions"`
	Granularity  string        `json:"granularity"`
	Aggregations []Aggregation `json:"aggregations"`
	Intervals    []string      `json:"intervals"`
	Filters      Filter        `json:"filter,omitempty"`
	LimitSpec    LimitSpec     `json:"limitSpec,omitempty"`
}

type TopNPayload struct {
	QueryType    string        `json:"queryType"`
	DataSource   string        `json:"dataSource"`
	Dimensions   string        `json:"dimension"`
	Granularity  string        `json:"granularity"`
	Aggregations []Aggregation `json:"aggregations"`
	Intervals    []string      `json:"intervals"`
	Filters      Filter        `json:"filter,omitempty"`
	LimitSpec    LimitSpec     `json:"limitSpec,omitempty"`
	Threshold    int           `json:"threshold"`
	Metric       string        `json:"metric"`
}

var aggregationName = ""

func (t *DruidDatasource) Query(ctx context.Context, tsdbReq *datasource.DatasourceRequest) (*datasource.DatasourceResponse, error) {

	t.logger.Debug("Query", "datasource", tsdbReq.Datasource.Name, "TimeRange", tsdbReq.TimeRange)
	var response *datasource.DatasourceResponse
	var err error

	modelJson, jsonerr := simplejson.NewJson([]byte(tsdbReq.Queries[0].ModelJson))
	if jsonerr != nil {
	}

	t.logger.Debug("Query", "Model Json type", reflect.TypeOf(modelJson))

	response, err = t.handleQuery(tsdbReq)
	if err != nil {
	}

	return response, nil
}
func (t *DruidDatasource) handleQuery(tsdbReq *datasource.DatasourceRequest) (*datasource.DatasourceResponse, error) {

	//Input data

	var FromEpochMs, error1 = strconv.ParseInt(strconv.FormatInt(tsdbReq.TimeRange.FromEpochMs, 10)[0:10], 10, 64)
	var ToEpochMs, error2 = strconv.ParseInt(strconv.FormatInt(tsdbReq.TimeRange.ToEpochMs, 10)[0:10], 10, 64)
	if error1 != nil {
	}
	if error2 != nil {
	}

	from := time.Unix(int64(FromEpochMs), 0).Format(time.RFC3339)[0:19] + "Z"
	to := time.Unix(int64(ToEpochMs), 0).Format(time.RFC3339)[0:19] + "Z"

	modelJson, jsonerr := simplejson.NewJson([]byte(tsdbReq.Queries[0].ModelJson))
	if jsonerr != nil {
	}

	t.logger.Debug("Query", "Model Json", modelJson)

	datasourceName := modelJson.Get("druidDS").MustString()
	granularity := modelJson.Get("customGranularity").MustString()
	queryType := modelJson.Get("queryType").MustString()
	aggregationName = modelJson.Get("aggregators").GetIndex(0).Get("name").MustString()
	aggregationType := modelJson.Get("aggregators").GetIndex(0).Get("type").MustString()
	aggregationFieldName := modelJson.Get("aggregators").GetIndex(0).Get("fieldName").MustString()

	filters_len := len(modelJson.Get("filters").MustArray())
	t.logger.Debug("debug", "filter len", filters_len)

	//Filters Spec

	var filter Filter

	if filters_len > 1 {
		filter.Type = "and"
		filter.Fields = make([]Fields, filters_len)
		for i := range filter.Fields {
			fieldType := modelJson.Get("filters").GetIndex(i).Get("type").MustString()
			filter.Fields[i] = Fields{
				FieldsType: fieldType,
				Dimension:  modelJson.Get("filters").GetIndex(i).Get("dimension").MustString(),
			}
			if fieldType == "selector" {
				filter.Fields[i].Value = modelJson.Get("filters").GetIndex(i).Get("value").MustString()
			} else if fieldType == "regex" {
				filter.Fields[i].Pattern = modelJson.Get("filters").GetIndex(i).Get("pattern").MustString()
			} else if fieldType == "in" {
				filter.Fields[i].InValues = strings.Split(modelJson.Get("filters").GetIndex(i).Get("values").MustString(), ",")

			}

		}
	}
	if filters_len == 1 {
		filter.Type = modelJson.Get("filters").GetIndex(0).Get("type").MustString()
		filter.Dimension = modelJson.Get("filters").GetIndex(0).Get("dimension").MustString()
		if filter.Type == "selector" {
			filter.Value = modelJson.Get("filters").GetIndex(0).Get("value").MustString()
		} else if filter.Type == "regex" {
			filter.Pattern = modelJson.Get("filters").GetIndex(0).Get("pattern").MustString()
		} else if filter.Type == "in" {
			filter.InValues = strings.Split(modelJson.Get("filters").GetIndex(0).Get("values").MustString(), ",")
		}
	}

	//Query

	var payloadBytes []byte
	var err error

	switch queryType {

	case "timeseries":
		{

			data := &TimeSeriesPayload{
				QueryType:    queryType,
				DataSource:   datasourceName,
				Granularity:  granularity,
				Aggregations: []Aggregation{{aggregationType, aggregationFieldName, aggregationName}},
				Intervals:    []string{from + "/" + to},
				Filters:      Filter{filter.Type, filter.Dimension, filter.Value, filter.Pattern, filter.InValues, filter.Fields},
			}

			payloadBytes, err = json.Marshal(data)
			if err != nil {
				println(err)
			}
		}

	case "groupBy":
		{

			dimensions := modelJson.Get("groupBy").MustArray()
			limit := modelJson.Get("limit").MustInt()
			orderby := modelJson.Get("orderBy").MustStringArray()

			var limitspec LimitSpec
			limitspec.Columns = make([]Column, len(orderby))

			for i := 0; i < len(orderby); i++ {
				limitspec.Columns[i] = Column{
					Dimension: orderby[i],
					Direction: "DESCENDING",
				}
			}

			//Limit and Order Spec

			limitspec.Limit = limit
			limitspec.Type = "default"

			data := &GroupByPayload{
				QueryType:    queryType,
				DataSource:   datasourceName,
				Dimensions:   dimensions,
				Granularity:  granularity,
				Aggregations: []Aggregation{{aggregationType, aggregationFieldName, aggregationName}},
				Intervals:    []string{from + "/" + to},
				Filters:      Filter{filter.Type, filter.Dimension, filter.Value, filter.Pattern, filter.InValues, filter.Fields},
				LimitSpec:    limitspec,
			}

			payloadBytes, err = json.Marshal(data)
			if err != nil {
				println(err)
			}
		}

	case "topN":
		{
			dimensions := modelJson.Get("dimension").MustString()
			limit := modelJson.Get("limit").MustInt()
			metric := modelJson.Get("druidMetric").MustString()

			data := &TopNPayload{
				QueryType:    queryType,
				DataSource:   datasourceName,
				Dimensions:   dimensions,
				Granularity:  granularity,
				Aggregations: []Aggregation{{aggregationType, aggregationFieldName, aggregationName}},
				Intervals:    []string{from + "/" + to},
				Filters:      Filter{filter.Type, filter.Dimension, filter.Value, filter.Pattern, filter.InValues, filter.Fields},
				Threshold:    limit,
				Metric:       metric,
			}

			payloadBytes, err = json.Marshal(data)
			if err != nil {
				println(err)
			}

		}

	}

	reqBody := string(payloadBytes)
	if filters_len == 0 {
		reg := regexp.MustCompile(`,"filter":{}`)
		reqBody = reg.ReplaceAllString(reqBody, "")
	}

	t.logger.Error("Query", "Payload", reqBody)
	url := tsdbReq.Datasource.Url + "/druid/v2"
	return t.makeRequest(reqBody, url, queryType)

}
func (t *DruidDatasource) makeRequest(reqBody string, url string, queryType string) (*datasource.DatasourceResponse, error) {

	//Request
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(reqBody))

	if err != nil {
		// handle err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		// handle err
		println("err in response")
	}
	t.logger.Debug("Query", "response payload", resp.Status)

	if resp.StatusCode != http.StatusOK {
		fmt.Errorf("invalid status code. status: %v", resp.Status)
	}
	return t.parseResponse(resp, queryType)
}
func (t *DruidDatasource) parseResponse(resp *http.Response, queryType string) (*datasource.DatasourceResponse, error) {
	//Response

	t.logger.Debug("parseResponse", "test", aggregationName)
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	t.logger.Debug("Query", "msg", body)

	//Response data parsing

	var res []map[string]interface{}
	json.Unmarshal(body, &res)

	t.logger.Debug("Query", "Response body", res)

	qr := datasource.QueryResult{
		Series: make([]*datasource.TimeSeries, 0),
	}

	serie := &datasource.TimeSeries{Name: "druid"}

	response_len := len(res)

	switch queryType {

	case "timeseries":
		for i := 0; i < response_len; i++ {

			timestamp, err := time.Parse(time.RFC3339, res[i]["timestamp"].(string))
			result := res[i]["result"]

			t.logger.Debug("Query", "Timestamp", timestamp)
			t.logger.Debug("Query", "Result", result)

			if err != nil {
				t.logger.Error("Query", "Response points err", err)
			}
			serie.Points = append(serie.Points, &datasource.Point{
				Timestamp: timestamp.Unix(),
				Value:     result.(map[string]interface{})[aggregationName].(float64),
			})
		}
	case "groupBy":
		for i := 0; i < response_len; i++ {

			timestamp, err := time.Parse(time.RFC3339, res[i]["timestamp"].(string))
			result := res[i]["event"]

			t.logger.Debug("Query", "Timestamp", timestamp)
			t.logger.Debug("Query", "Result", result)

			if err != nil {
				t.logger.Error("Query", "Response points err", err)
			}
			serie.Points = append(serie.Points, &datasource.Point{
				Timestamp: timestamp.Unix(),
				Value:     result.(map[string]interface{})[aggregationName].(float64),
			})
		}

	case "topN":
		for i := 0; i < response_len; i++ {

			timestamp, err := time.Parse(time.RFC3339, res[i]["timestamp"].(string))
			result := res[i]["result"]

			t.logger.Debug("Query", "Timestamp", timestamp)
			t.logger.Debug("Query", "Result", result)
			t.logger.Debug("test", "len", reflect.ValueOf(result).Len())
			var resultMap reflect.Value
			var value float64
			if reflect.ValueOf(result).Len() > 0 {
				resultMap = reflect.ValueOf(result).Index(0)
				value = resultMap.Interface().(map[string]interface{})[aggregationName].(float64)
			}

			if err != nil {
				t.logger.Error("Query", "Response points err", err)
			}
			serie.Points = append(serie.Points, &datasource.Point{
				Timestamp: timestamp.Unix(),
				Value:     value,
			})
		}
	}

	qr.Series = append(qr.Series, serie)
	response := &datasource.DatasourceResponse{}
	response.Results = append(response.Results, &qr)

	return response, nil
}

var httpClient = &http.Client{
	Transport: &http.Transport{
		TLSClientConfig: &tls.Config{
			Renegotiation: tls.RenegotiateFreelyAsClient,
		},
		Proxy: http.ProxyFromEnvironment,
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).Dial,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
	},
	Timeout: time.Duration(time.Second * 30),
}
